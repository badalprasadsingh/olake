package olake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"

	// "github.com/datazip-inc/olake/typeutils"
	"github.com/google/uuid"
)

func (w *NewIcebergGo) Setup(stream protocol.Stream, options *protocol.Options) error {
	w.stream = stream
	w.records = make([]types.RawRecord, 0, w.config.BatchSize) // why is 0 passed here?:
	w.allocator = memory.NewGoAllocator()
	w.schemaMapping = make(map[string]int) // Initialize the schema mapping

	logger.Infof("Setting up ICEBERGGO writer with catalog %s at %s",
		w.config.CatalogType, w.config.RestCatalogURL)
	logger.Infof("S3 endpoint: %s, region: %s", w.config.S3Endpoint, w.config.AwsRegion)
	logger.Infof("Iceberg DB: %s, namespace: %s", w.config.IcebergDB, w.config.Namespace)
	logger.Infof("Handling nil values with appropriate default values (0 for numbers, empty string for text)")

	ctx := context.Background()
	s3Endpoint := w.config.S3Endpoint
	if s3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}

	// Debug logging for credentials
	logger.Infof("Using explicit S3 credentials - Access Key: %s, Secret Key: %s[redacted]",
		w.config.AwsAccessKey,
		w.config.AwsSecretKey[:1])

	w.s3Client = s3.New(s3.Options{
		Region: w.config.AwsRegion,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			w.config.AwsAccessKey,
			w.config.AwsSecretKey,
			"",
		)),
		EndpointResolver: s3.EndpointResolverFunc(func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               s3Endpoint,
				HostnameImmutable: true,
				SigningRegion:     w.config.AwsRegion,
			}, nil
		}),
		UsePathStyle: w.config.S3PathStyle,
		// Disable EC2 IMDS credential provider retries
		ClientLogMode:    aws.LogRetries,
		RetryMaxAttempts: 1,
	})

	// Test S3 connectivity
	logger.Infof("Testing S3 connectivity to %s", s3Endpoint)
	_, listErr := w.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if listErr != nil {
		logger.Errorf("S3 connectivity test failed: %v", listErr)
		return fmt.Errorf("S3 connectivity test failed, please check your credentials and endpoint: %v", listErr)
	}
	logger.Infof("S3 connectivity test successful")

	props := iceberg.Properties{
		iceio.S3Region:             w.config.AwsRegion,
		iceio.S3AccessKeyID:        w.config.AwsAccessKey,
		iceio.S3SecretAccessKey:    w.config.AwsSecretKey,
		iceio.S3EndpointURL:        w.config.S3Endpoint,
		"warehouse":                "s3://warehouse/",
		"s3.path-style-access":     "true",
		"s3.connection-timeout-ms": "50000",
		"s3.socket-timeout-ms":     "50000",
	}

	if w.config.S3PathStyle {
		props[iceio.S3ForceVirtualAddressing] = "false"
	}

	restCatalog, err := rest.NewCatalog(
		ctx,
		"olake-catalog",
		w.config.RestCatalogURL,
		rest.WithOAuthToken(""), // Add token if needed
	)

	if err != nil {
		return fmt.Errorf("failed to create REST catalog: %v", err)
	}

	w.catalog = restCatalog // created a new catalog and added it to the config

	schemaFields, err := w.createIcebergSchema2()
	if err != nil {
		return fmt.Errorf("failed to create iceberg schema: %v", err)
	}

	w.schema = iceberg.NewSchema(0, schemaFields...)
	w.tableIdent = catalog.ToIdentifier(w.config.Namespace, w.stream.Name())

	var tableExists bool
	_, err = w.catalog.LoadTable(ctx, w.tableIdent, props)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			tableExists = false
			logger.Infof("Table %s does not exist", w.tableIdent)
		}
	} else {
		tableExists = true
		logger.Infof("Table %s exists", w.tableIdent)
	}

	if !tableExists {
		if !w.config.CreateTableIfNotExists {
			return fmt.Errorf("table %s does not exist and create_table_if_not_exists is false", w.tableIdent)
		}

		logger.Infof("Creating new Iceberg table: %s", w.tableIdent)
		w.iceTable, err = w.catalog.CreateTable(ctx, w.tableIdent, w.schema, catalog.WithProperties(iceberg.Properties{
			"write.format.default":  "parquet",
			iceio.S3Region:          w.config.AwsRegion,
			iceio.S3AccessKeyID:     w.config.AwsAccessKey,
			iceio.S3SecretAccessKey: w.config.AwsSecretKey,
			iceio.S3EndpointURL:     w.config.S3Endpoint,
			"s3.path-style-access":  "true",
		}))
		if err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
	} else {
		logger.Infof("Loading existing Iceberg table: %s", w.tableIdent)
		w.iceTable, err = w.catalog.LoadTable(ctx, w.tableIdent, props)
		if err != nil {
			return fmt.Errorf("failed to load table: %v", err)
		}
	}

	// Initialize record builder
	w.createRecordBuilder()

	return nil
}

func (w *NewIcebergGo) createIcebergSchema2() ([]iceberg.NestedField, error) {
	if w.stream == nil || w.stream.Schema() == nil {
		return nil, fmt.Errorf("stream or schema is nil")
	}

	streamSchema := w.stream.Schema()
	fields := make([]iceberg.NestedField, 0)

	streamSchema.Properties.Range(func(key, value interface{}) bool {
		name := key.(string)
		// property := value.(*types.Property)

		var fieldType iceberg.Type

		// dataType := property.DataType()
		fieldType = iceberg.StringType{}

		fields = append(fields, iceberg.NestedField{
			Name:     name,
			Type:     fieldType,
			Required: false,
		})

		w.schemaMapping[name] = 1
		return true
	})

	return fields, nil
}

func (w *NewIcebergGo) createRecordBuilder() {
	fields := make([]arrow.Field, 0, len(w.schema.Fields()))

	for _, field := range w.schema.Fields() {
		var arrowType arrow.DataType
		arrowType = arrow.BinaryTypes.String

		fields = append(fields, arrow.Field{
			Name:     field.Name,
			Type:     arrowType,
			Nullable: !field.Required,
		})
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	logger.Infof("Arrow schema: %v", arrowSchema)
	w.recordBuilder = array.NewRecordBuilder(w.allocator, arrowSchema)
}

func (w *NewIcebergGo) Write(_ context.Context, record types.RawRecord) error {
	w.recordsMutex.Lock()
	defer w.recordsMutex.Unlock()

	w.records = append(w.records, record)
	if len(w.records) >= w.config.BatchSize {
		return w.flushRecords()
	}
	return nil
}

func (w *NewIcebergGo) flushRecords() error {
	if len(w.records) == 0 {
		return nil
	}
	logger.Infof("Flushing %d records to Iceberg", len(w.records))

	// Create a new record builder for each flush
	schema := w.recordBuilder.Schema()
	w.recordBuilder = array.NewRecordBuilder(w.allocator, schema)

	// Reserve space for all records
	w.recordBuilder.Reserve(len(w.records))

	// Get all field builders
	numFields := len(schema.Fields())
	fieldBuilders := make([]array.Builder, numFields)
	for i := 0; i < numFields; i++ {
		fieldBuilders[i] = w.recordBuilder.Field(i)
	}

	// Create field name to index mapping
	fieldNameToIndex := make(map[string]int)
	for name, id := range w.schemaMapping {
		fieldNameToIndex[name] = id - 1 // Adjust for 0-based indexing
	}

	// Process records
	for _, record := range w.records {
		// Process each field in the schema to ensure consistent counts
		for i := 0; i < numFields; i++ {
			fieldBuilder := fieldBuilders[i]
			fieldName := schema.Field(i).Name

			value, exists := record.Data[fieldName]
			if !exists || value == nil {
				fieldBuilder.AppendNull()
				continue
			}

			// Convert and append the value
			switch builder := fieldBuilder.(type) {
			case *array.StringBuilder:
				if strVal, ok := toString(value); ok {
					builder.Append(strVal)
				} else {
					builder.Append("")
				}
			case *array.Int32Builder:
				if intVal, ok := toInt32(value); ok {
					builder.Append(intVal)
				} else {
					builder.Append(0)
				}
			case *array.Int64Builder:
				if intVal, ok := toInt64(value); ok {
					builder.Append(intVal)
				} else {
					builder.Append(0)
				}
			case *array.Float32Builder:
				if floatVal, ok := toFloat32(value); ok {
					builder.Append(floatVal)
				} else {
					builder.Append(0.0)
				}
			case *array.Float64Builder:
				if floatVal, ok := toFloat64(value); ok {
					builder.Append(floatVal)
				} else {
					builder.Append(0.0)
				}
			case *array.BooleanBuilder:
				if boolVal, ok := value.(bool); ok {
					builder.Append(boolVal)
				} else {
					builder.Append(false)
				}
			case *array.TimestampBuilder:
				if timeVal, ok := value.(time.Time); ok {
					builder.Append(arrow.Timestamp(timeVal.UnixMicro()))
				} else if strVal, ok := value.(string); ok {
					if timeVal, err := time.Parse(time.RFC3339, strVal); err == nil {
						builder.Append(arrow.Timestamp(timeVal.UnixMicro()))
					} else {
						builder.Append(arrow.Timestamp(time.Now().UnixMicro()))
					}
				} else {
					builder.Append(arrow.Timestamp(time.Now().UnixMicro()))
				}
			default:
				// For any other type, append null
				fieldBuilder.AppendNull()
			}
		}
	}

	// Verify all fields have the same length before creating the record
	rowCount := fieldBuilders[0].Len()
	for i := 1; i < numFields; i++ {
		if fieldBuilders[i].Len() != rowCount {
			logger.Errorf("Field %d has %d rows, expected %d rows", i, fieldBuilders[i].Len(), rowCount)
			// Fix by appending nulls to match row count
			for fieldBuilders[i].Len() < rowCount {
				fieldBuilders[i].AppendNull()
			}
		}
	}

	// Create the record
	record := w.recordBuilder.NewRecord()
	defer record.Release()

	// Create Arrow table
	arrowTable := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
	defer arrowTable.Release()

	ctx := context.Background()

	// Add retry logic for transaction conflicts
	maxRetries := 3
	var updatedTable *table.Table
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Start a new transaction with the latest table state
		if attempt > 0 {
			// Reload the table to get the latest state before retry
			w.iceTable, err = w.catalog.LoadTable(ctx, w.tableIdent, iceberg.Properties{})
			if err != nil {
				return fmt.Errorf("failed to reload table on retry %d: %v", attempt, err)
			}
			logger.Infof("Retrying transaction (attempt %d/%d) after conflict", attempt+1, maxRetries)
		}

		txn := w.iceTable.NewTransaction()

		// Generate a unique file path for this batch
		fileUUID := uuid.New().String()
		filePath := fmt.Sprintf("s3://warehouse/%s/%s/data-%s.parquet",
			w.config.Namespace, w.stream.Name(), fileUUID)

		// Write record to a parquet file using the table's filesystem
		writeFileIO, ok := w.iceTable.FS().(iceio.WriteFileIO)
		if !ok {
			return fmt.Errorf("filesystem does not support writing")
		}

		// Create parquet file with the record
		fw, err := writeFileIO.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create parquet file: %v", err)
		}

		// Write the record to parquet
		if err := pqarrow.WriteTable(arrowTable, fw, record.NumRows(), nil, pqarrow.DefaultWriterProps()); err != nil {
			fw.Close()
			return fmt.Errorf("failed to write record to parquet: %v", err)
		}

		if err := fw.Close(); err != nil {
			return fmt.Errorf("failed to close parquet file: %v", err)
		}

		// Add the data file to the transaction
		err = txn.AddFiles([]string{filePath}, iceberg.Properties{}, false)
		if err != nil {
			return fmt.Errorf("failed to add file to transaction: %v", err)
		}

		// Try to commit the transaction
		updatedTable, err = txn.Commit(ctx)
		if err != nil {
			// Check if it's a conflict error
			if strings.Contains(err.Error(), "branch main has changed") ||
				strings.Contains(err.Error(), "CommitFailedException") {
				logger.Warnf("Transaction conflict detected: %v, will retry", err)
				continue // Try again
			}
			// Non-conflict error, return immediately
			return fmt.Errorf("failed to commit transaction: %v", err)
		}

		// Success! Break out of the retry loop
		break
	}

	// If we exhausted retries, check if we have an error
	if err != nil {
		return fmt.Errorf("failed to commit transaction after %d retries: %v", maxRetries, err)
	}

	// Update our table reference to the latest version
	w.iceTable = updatedTable

	// Clear the batch
	w.records = w.records[:0]

	return nil
}
