package new_iceberggo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
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

// A simple implementation of the writer interface for ICEBERGGO
type NewIcebergGo struct {
	config *Config
	stream protocol.Stream
	
	// Iceberg components
	catalog    catalog.Catalog
	iceTable   *table.Table
	schema     *iceberg.Schema
	tableIdent table.Identifier
	
	// Batching
	records       []types.RawRecord
	recordsMutex  sync.Mutex
	
	// Arrow components
	allocator     memory.Allocator
	recordBuilder *array.RecordBuilder
	
	// For column mapping
	schemaMapping map[string]int
	
	// S3 client for Iceberg operations
	s3Client *s3.Client
}

type Config struct {
	// Catalog configuration
	CatalogType    string `json:"catalog_type"`
	RestCatalogURL string `json:"rest_catalog_url"`

	// S3 configuration
	S3Endpoint   string `json:"s3_endpoint"`
	AwsRegion    string `json:"aws_region"`
	AwsAccessKey string `json:"aws_access_key"`
	AwsSecretKey string `json:"aws_secret_key"`
	S3UseSSL     bool   `json:"s3_use_ssl"`
	S3PathStyle  bool   `json:"s3_path_style"`

	// Iceberg configuration
	IcebergDB string `json:"iceberg_db"`
	Namespace string `json:"namespace"`
	
	// Table options
	CreateTableIfNotExists bool `json:"create_table_if_not_exists"`
	BatchSize             int   `json:"batch_size"`
	Normalization         bool  `json:"normalization"`
}

// GetConfigRef returns a reference to this writer's configuration
func (w *NewIcebergGo) GetConfigRef() protocol.Config {
	w.config = &Config{}
	return w.config
}

// Spec returns the configuration specification
func (w *NewIcebergGo) Spec() any {
	return Config{}
}

// Helpers for type conversion
func toInt32(value any) (int32, bool) {
	switch v := value.(type) {
	case int:
		return int32(v), true
	case int8:
		return int32(v), true
	case int16:
		return int32(v), true
	case int32:
		return v, true
	case int64:
		if v > 2147483647 || v < -2147483648 {
			return 0, false
		}
		return int32(v), true
	case uint8:
		return int32(v), true
	case uint16:
		return int32(v), true
	case uint32:
		if v > 2147483647 {
			return 0, false
		}
		return int32(v), true
	case float32:
		return int32(v), true
	case float64:
		return int32(v), true
	case string:
		if i, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int32(i), true
		}
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func toInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > 9223372036854775807 {
			return 0, false
		}
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, true
		}
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func toFloat32(value any) (float32, bool) {
	switch v := value.(type) {
	case int:
		return float32(v), true
	case int8:
		return float32(v), true
	case int16:
		return float32(v), true
	case int32:
		return float32(v), true
	case int64:
		return float32(v), true
	case uint8:
		return float32(v), true
	case uint16:
		return float32(v), true
	case uint32:
		return float32(v), true
	case uint64:
		return float32(v), true
	case float32:
		return v, true
	case float64:
		return float32(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 32); err == nil {
			return float32(f), true
		}
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	}
	return 0.0, false
}

func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	}
	return 0.0, false
}

func toString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprintf("%v", v), true
	case time.Time:
		return v.Format(time.RFC3339), true
	}
	return "", false
}

// Setup initializes the writer
func (w *NewIcebergGo) Setup(stream protocol.Stream, options *protocol.Options) error {
	w.stream = stream
	w.records = make([]types.RawRecord, 0, w.config.BatchSize)
	w.allocator = memory.NewGoAllocator()
	
	// Log configuration details
	logger.Infof("Setting up ICEBERGGO writer with catalog %s at %s", 
		w.config.CatalogType, w.config.RestCatalogURL)
	logger.Infof("S3 endpoint: %s, region: %s", w.config.S3Endpoint, w.config.AwsRegion)
	logger.Infof("Iceberg DB: %s, namespace: %s", w.config.IcebergDB, w.config.Namespace)
	
	ctx := context.Background()
	
	// Configure S3 client
	s3Endpoint := w.config.S3Endpoint
	if s3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}
	
	// Create S3 client
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
	})
	
	// Create REST catalog with proper S3 properties
	props := iceberg.Properties{
		iceio.S3Region:          w.config.AwsRegion,
		iceio.S3AccessKeyID:     w.config.AwsAccessKey,
		iceio.S3SecretAccessKey: w.config.AwsSecretKey,
		iceio.S3EndpointURL:     w.config.S3Endpoint,
		"warehouse":             "s3://warehouse/iceberg/",
	}
	
	if w.config.S3PathStyle {
		props[iceio.S3ForceVirtualAddressing] = "false"
	}
	
	restCatalog, err := rest.NewCatalog(
		ctx, 
		"olake-catalog", 
		w.config.RestCatalogURL,
		rest.WithOAuthToken(""),  // Add token if needed
	)
	
	if err != nil {
		return fmt.Errorf("failed to create REST catalog: %v", err)
	}
	
	w.catalog = restCatalog
	
	// Create schema from stream schema
	schemaFields, err := w.createIcebergSchema()
	if err != nil {
		return fmt.Errorf("failed to create iceberg schema: %v", err)
	}
	
	// Create schema with ID 0 (since it will be assigned by Iceberg)
	w.schema = iceberg.NewSchema(0, schemaFields...)
	
	// Set up table identifier
	w.tableIdent = catalog.ToIdentifier(w.config.Namespace, w.stream.Name())
	
	// Load table if it exists, otherwise create it
	var tableExists bool
	_, err = w.catalog.LoadTable(ctx, w.tableIdent, props)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			tableExists = false
			logger.Infof("Table %s does not exist", w.tableIdent)
		} else {
			return fmt.Errorf("failed to check if table exists: %v", err)
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
			"write.format.default": "parquet",
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

// createIcebergSchema converts the stream schema to Iceberg schema fields
func (w *NewIcebergGo) createIcebergSchema() ([]iceberg.NestedField, error) {
	if w.stream == nil || w.stream.Schema() == nil {
		return nil, fmt.Errorf("stream or schema is nil")
	}
	
	streamSchema := w.stream.Schema()
	fields := make([]iceberg.NestedField, 0)
	fieldID := 1
	
	// Create the schema mapping
	w.schemaMapping = make(map[string]int)
	
	// Add olake_id field first
	fields = append(fields, iceberg.NestedField{
		ID:       fieldID,
		Name:     "_olake_id",
		Type:     iceberg.StringType{},
		Required: true,
	})
	w.schemaMapping["_olake_id"] = fieldID
	fieldID++
	
	// Add data fields
	streamSchema.Properties.Range(func(key, value interface{}) bool {
		name := key.(string)
		property := value.(*types.Property)
		
		var fieldType iceberg.Type
		
		// Map olake types to iceberg types
		dataType := property.DataType()
		switch dataType {
		case types.Int32:
			fieldType = iceberg.Int32Type{}
		case types.Int64:
			fieldType = iceberg.Int64Type{}
		case types.Float32:
			fieldType = iceberg.Float32Type{}
		case types.Float64:
			fieldType = iceberg.Float64Type{}
		case types.String:
			fieldType = iceberg.StringType{}
		case types.Bool:
			fieldType = iceberg.BooleanType{}
		case types.Timestamp, types.TimestampMilli, types.TimestampMicro, types.TimestampNano:
			fieldType = iceberg.TimestampType{}
		default:
			fieldType = iceberg.StringType{}
		}
		
		// Check if the field is nullable
		isNullable := property.Nullable()
		
		fields = append(fields, iceberg.NestedField{
			ID:       fieldID,
			Name:     name,
			Type:     fieldType,
			Required: !isNullable,
		})
		
		w.schemaMapping[name] = fieldID
		fieldID++
		return true
	})
	
	// Add metadata fields
	fields = append(fields, iceberg.NestedField{
		ID:       fieldID,
		Name:     "_olake_timestamp",
		Type:     iceberg.TimestampType{},
		Required: true,
	})
	w.schemaMapping["_olake_timestamp"] = fieldID
	fieldID++
	
	fields = append(fields, iceberg.NestedField{
		ID:       fieldID,
		Name:     "_op_type",
		Type:     iceberg.StringType{},
		Required: true,
	})
	w.schemaMapping["_op_type"] = fieldID
	fieldID++
	
	fields = append(fields, iceberg.NestedField{
		ID:       fieldID,
		Name:     "_cdc_timestamp",
		Type:     iceberg.TimestampType{},
		Required: true,
	})
	w.schemaMapping["_cdc_timestamp"] = fieldID
	
	return fields, nil
}

// createRecordBuilder initializes the Arrow record builder
func (w *NewIcebergGo) createRecordBuilder() {
	// Convert Iceberg schema to Arrow schema
	arrowSchema, err := table.SchemaToArrowSchema(w.schema, nil, true, false)
	if err != nil {
		logger.Errorf("Failed to convert schema to Arrow schema: %v", err)
		return
	}
	
	// Create a new record builder with the arrow schema
	w.recordBuilder = array.NewRecordBuilder(w.allocator, arrowSchema)
}

// Write handles writing a record
func (w *NewIcebergGo) Write(_ context.Context, record types.RawRecord) error {
	w.recordsMutex.Lock()
	defer w.recordsMutex.Unlock()
	
	// Add the record to the batch
	w.records = append(w.records, record)
	
	// If we've reached the batch size, flush the records
	if len(w.records) >= w.config.BatchSize {
		return w.flushRecords()
	}
	
	return nil
}

// flushRecords writes the accumulated records to Iceberg
func (w *NewIcebergGo) flushRecords() error {
	if len(w.records) == 0 {
		return nil
	}
	
	logger.Infof("Flushing %d records to Iceberg", len(w.records))
	
	// Reset the record builder
	w.recordBuilder.Reserve(len(w.records))
	
	// Get field builders for metadata fields
	olakeIDBuilder := w.recordBuilder.Field(w.schemaMapping["_olake_id"] - 1).(*array.StringBuilder)
	olakeTimestampBuilder := w.recordBuilder.Field(w.schemaMapping["_olake_timestamp"] - 1).(*array.TimestampBuilder)
	opTypeBuilder := w.recordBuilder.Field(w.schemaMapping["_op_type"] - 1).(*array.StringBuilder)
	cdcTimestampBuilder := w.recordBuilder.Field(w.schemaMapping["_cdc_timestamp"] - 1).(*array.TimestampBuilder)
	
	// Map of field builders for data fields
	fieldBuilders := make(map[string]array.Builder)
	
	// Initialize field builders for all columns
	for name, id := range w.schemaMapping {
		if name != "_olake_id" && name != "_olake_timestamp" && name != "_op_type" && name != "_cdc_timestamp" {
			fieldBuilders[name] = w.recordBuilder.Field(id - 1)
		}
	}
	
	// Populate the record builder with data
	for _, record := range w.records {
		// Add metadata fields
		olakeIDBuilder.Append(record.OlakeID)
		olakeTimestampBuilder.Append(arrow.Timestamp(record.OlakeTimestamp.UnixNano()))
		opTypeBuilder.Append(record.OperationType)
		cdcTimestampBuilder.Append(arrow.Timestamp(record.CdcTimestamp.UnixNano()))
		
		// Add data fields
		for fieldName, fieldBuilder := range fieldBuilders {
			value, exists := record.Data[fieldName]
			
			if !exists || value == nil {
				// Handle null values based on the type of the builder
				fieldBuilder.AppendNull()
				continue
			}
			
			// Append the value based on the builder type
			switch builder := fieldBuilder.(type) {
			case *array.Int32Builder:
				if intVal, ok := toInt32(value); ok {
					builder.Append(intVal)
				} else {
					builder.AppendNull()
				}
			case *array.Int64Builder:
				if intVal, ok := toInt64(value); ok {
					builder.Append(intVal)
				} else {
					builder.AppendNull()
				}
			case *array.Float32Builder:
				if floatVal, ok := toFloat32(value); ok {
					builder.Append(floatVal)
				} else {
					builder.AppendNull()
				}
			case *array.Float64Builder:
				if floatVal, ok := toFloat64(value); ok {
					builder.Append(floatVal)
				} else {
					builder.AppendNull()
				}
			case *array.BooleanBuilder:
				if boolVal, ok := value.(bool); ok {
					builder.Append(boolVal)
				} else {
					builder.AppendNull()
				}
			case *array.StringBuilder:
				if strVal, ok := toString(value); ok {
					builder.Append(strVal)
				} else {
					builder.AppendNull()
				}
			case *array.TimestampBuilder:
				if timeVal, ok := value.(time.Time); ok {
					builder.Append(arrow.Timestamp(timeVal.UnixNano()))
				} else if strVal, ok := value.(string); ok {
					if timeVal, err := time.Parse(time.RFC3339, strVal); err == nil {
						builder.Append(arrow.Timestamp(timeVal.UnixNano()))
					} else {
						builder.AppendNull()
					}
				} else {
					builder.AppendNull()
				}
			default:
				// For unsupported types, try to convert to string
				if strVal, ok := toString(value); ok {
					if strBuilder, ok := builder.(*array.StringBuilder); ok {
						strBuilder.Append(strVal)
					} else {
						builder.AppendNull()
					}
				} else {
					builder.AppendNull()
				}
			}
		}
	}
	
	// Create record batch
	record := w.recordBuilder.NewRecord()
	defer record.Release()
	
	// Get a transaction 
	ctx := context.Background()
	txn := w.iceTable.NewTransaction()
	
	// Generate a unique file path for this batch
	fileUUID := uuid.New().String()
	filePath := fmt.Sprintf("s3://warehouse/iceberg/%s/%s/data-%s.parquet", 
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
	if err := pqarrow.WriteTable(array.NewTableFromRecords(record.Schema(), []arrow.Record{record}), 
		fw, record.NumRows(), nil, pqarrow.DefaultWriterProps()); err != nil {
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
	
	// Commit the transaction to finalize the write
	updatedTable, err := txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	
	// Update our table reference to the latest version
	w.iceTable = updatedTable
	
	// Clear the batch
	w.records = w.records[:0]
	
	return nil
}

// Close handles cleanup
func (w *NewIcebergGo) Close() error {
	logger.Infof("Closing ICEBERGGO writer")
	
	// Flush any remaining records
	w.recordsMutex.Lock()
	defer w.recordsMutex.Unlock()
	
	if len(w.records) > 0 {
		err := w.flushRecords()
		if err != nil {
			return fmt.Errorf("failed to flush records during close: %v", err)
		}
	}
	
	// Clean up resources
	if w.recordBuilder != nil {
		w.recordBuilder.Release()
	}
	
	return nil
}

// Check validates the configuration
func (w *NewIcebergGo) Check() error {
	logger.Infof("Checking ICEBERGGO writer configuration")
	
	// Validate basic requirements
	if w.config.RestCatalogURL == "" {
		return fmt.Errorf("rest_catalog_url is required")
	}
	if w.config.S3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}
	if w.config.AwsRegion == "" {
		return fmt.Errorf("aws_region is required")
	}
	if w.config.AwsAccessKey == "" {
		return fmt.Errorf("aws_access_key is required")
	}
	if w.config.AwsSecretKey == "" {
		return fmt.Errorf("aws_secret_key is required")
	}
	if w.config.IcebergDB == "" {
		return fmt.Errorf("iceberg_db is required")
	}
	
	return nil
}

// ReInitiationRequiredOnSchemaEvolution returns whether the writer should be re-initialized on schema evolution
func (w *NewIcebergGo) ReInitiationRequiredOnSchemaEvolution() bool {
	return true
}

// Type returns the writer type
func (w *NewIcebergGo) Type() string {
	return "iceberggo"
}

// Flattener returns a function to flatten records
func (w *NewIcebergGo) Flattener() protocol.FlattenFunction {
	return func(rec types.Record) (types.Record, error) {
		return rec, nil
	}
}

// Normalization returns whether normalization is enabled
func (w *NewIcebergGo) Normalization() bool {
	return w.config.Normalization
}

// EvolveSchema handles schema evolution
func (w *NewIcebergGo) EvolveSchema(_ bool, _ bool, _ map[string]*types.Property, _ types.Record, _ time.Time) error {
	// TODO: Implement schema evolution. For now, we'll require reinitialization
	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate basic requirements
	if c.RestCatalogURL == "" {
		return fmt.Errorf("rest_catalog_url is required")
	}
	if c.S3Endpoint == "" {
		return fmt.Errorf("s3_endpoint is required")
	}
	if c.AwsRegion == "" {
		return fmt.Errorf("aws_region is required")
	}
	if c.AwsAccessKey == "" {
		return fmt.Errorf("aws_access_key is required")
	}
	if c.AwsSecretKey == "" {
		return fmt.Errorf("aws_secret_key is required")
	}
	if c.IcebergDB == "" {
		return fmt.Errorf("iceberg_db is required")
	}
	if c.Namespace == "" {
		c.Namespace = c.IcebergDB // Use IcebergDB as namespace if not specified
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000 // Default batch size
	}
	return nil
}

func init() {
	protocol.RegisteredWriters[types.IcebergGo] = func() protocol.Writer {
		return new(NewIcebergGo)
	}
} 