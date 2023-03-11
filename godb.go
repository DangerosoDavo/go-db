package godb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
)

type GoDB struct {
	ReadPool  *ConnectionPool
	WritePool *ConnectionPool
}

type TableInfo struct {
	Name    string
	Columns []string
	Keys    []string
}

type Where struct {
	Operator string
	Value    interface{}
}

func (g *GoDB) CreateOrUpdateTable(table interface{}) error {
	connPool := g.GetConnectionPool(GetWriteConn)
	conn, err := connPool.getConn()
	if err != nil {
		return err
	}
	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	structName := reflect.TypeOf(table).Elem().Name()

	name := Pluralize(structName)

	stmt, err := tx.Prepare(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY AUTO_INCREMENT)", name))
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	stmt, err = tx.Prepare(fmt.Sprintf("DESCRIBE %s", name))
	if err != nil {
		return err
	}

	var existingColumns []string
	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var column, typ, null, key, extra sql.NullString
		err = rows.Scan(&column, &typ, &null, &key, &extra)
		if err != nil {
			return err
		}
		existingColumns = append(existingColumns, column.String)
	}

	fields, _ := getFields(table)

	for _, f := range fields {
		if f.PrimaryKey && (f.Type == "INT" || f.Type == "BIGINT") {
			_, err = stmt.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s AUTO_INCREMENT NOT NULL", name, f.Name, f.Type))
		} else {
			_, err = stmt.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", name, f.Name, f.Type))
		}
		if err != nil {
			if !strings.Contains(err.Error(), "Duplicate column name") {
				return err
			} else {
				for _, col := range existingColumns {
					if col == f.Name {
						existingColumns = existingColumns[1:]
						break
					}
				}
				log.Printf("Warning: column %s exists in table but not in struct\n", f.Name)
			}
		}
	}

	for _, col := range existingColumns {
		stmt, err = tx.Prepare(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", name, col))
		if err != nil {
			return err
		}
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
		log.Printf("Warning: column %s exists in table but not in struct and has been dropped\n", col)
	}

	return tx.Commit()
}

func (g *GoDB) CreateOrUpdateTableRaw(tableName string, fields []Field) error {
	connPool := g.GetConnectionPool(GetWriteConn)
	if connPool == nil {
		return errors.New("no write connection available")
	}

	// Check if the table already exists
	if g.TableExists(tableName) {
		// Get the current columns in the table
		currentColumns, err := g.GetTableColumns(tableName)
		if err != nil {
			return fmt.Errorf("error getting current columns for table %s: %s", tableName, err)
		}

		// Check if any columns need to be added
		addColumns := make([]string, 0)
		for _, field := range fields {
			if !stringInSlice(field.Name, currentColumns) {
				if field.PrimaryKey {
					aiModifier := ""
					if field.Type == "INT" || field.Type == "BIGINT" {
						aiModifier = " AUTO_INCREMENT"
					}
					addColumns = append(addColumns, fmt.Sprintf("%s %s PRIMARY KEY%s", field.Name, field.Type, aiModifier))
				} else if field.SecondaryKey {
					addColumns = append(addColumns, fmt.Sprintf("%s %s KEY", field.Name, field.Type))
				} else {
					addColumns = append(addColumns, fmt.Sprintf("%s %s", field.Name, field.Type))
				}
			}
		}
		if len(addColumns) > 0 {
			query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableName, strings.Join(addColumns, ", "))
			if _, err := connPool.Exec(query); err != nil {
				return fmt.Errorf("error adding columns to table %s: %s", tableName, err)
			}
		}

		// Check if any columns need to be removed
		removeColumns := make([]string, 0)
		for _, column := range currentColumns {
			exists := false
			for _, field := range fields {
				if column == field.Name {
					exists = true
					break
				}
			}
			if !exists {
				removeColumns = append(removeColumns, column)
			}
		}
		if len(removeColumns) > 0 {
			query := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableName, strings.Join(removeColumns, ", "))
			if _, err := connPool.Exec(query); err != nil {
				return fmt.Errorf("error removing columns from table %s: %s", tableName, err)
			}
		}

		return nil
	}

	// Construct the CREATE TABLE query
	columnDefs := make([]string, 0)
	for _, field := range fields {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", field.Name, field.Type))
	}
	query := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(columnDefs, ", "))

	// Create the table
	if _, err := connPool.Exec(query); err != nil {
		return fmt.Errorf("error creating table %s: %s", tableName, err)
	}

	return nil
}

type Field struct {
	Name         string
	Type         string
	PrimaryKey   bool
	SecondaryKey bool
}

type FieldValue struct {
	Field *Field
	Value interface{}
}

func getFields(data interface{}) ([]Field, []interface{}) {
	var fields []Field
	val := reflect.ValueOf(data).Elem()
	typ := val.Type()
	values := make([]interface{}, 0, val.NumField())

	for i := 0; i < val.NumField(); i++ {
		t := typ.Field(i)
		if isPublic(t) {
			f := val.Field(i)
			keyTag := t.Tag.Get("GoDBKey")
			newField := Field{Name: t.Name, Type: sqlType(f.Type())}
			if keyTag == "primary" {
				newField.PrimaryKey = true
			} else if keyTag == "secondary" {
				newField.SecondaryKey = true
			}
			fields = append(fields, newField)
			values = append(values, f.Interface())
		}
	}

	return fields, values
}

func isPublic(f reflect.StructField) bool {
	return f.PkgPath == ""
}

func sqlType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INT"
	case reflect.Int64, reflect.Uint64:
		return "BIGINT"
	case reflect.Float32, reflect.Float64:
		return "DOUBLE"
	case reflect.String:
		return "VARCHAR(255)"
	default:
		return ""
	}
}

type Upsertable interface {
	GetIDColumns() []string
}

func (g *GoDB) Upsert(records []interface{}) error {
	if len(records) == 0 {
		return nil
	}

	connPool := g.GetConnectionPool(GetWriteConn)
	conn, err := connPool.getConn()
	if err != nil {
		return err
	}

	var lastRecordName string
	var columns []Field
	var values []interface{}
	var stmt *sql.Stmt

	for _, record := range records {
		// Get the table name
		recordName := reflect.TypeOf(records[0]).Elem().Name()
		if lastRecordName != "" && recordName != lastRecordName {
			return fmt.Errorf("cannot upsert multiple record types in a single call")
		}
		lastRecordName = recordName
		tableName := Pluralize(strings.ToLower(recordName))

		columns, values = getFields(record)

		// Prepare the statement for upserting the records
		valuePlaceholders := strings.Repeat("(?),", len(columns))
		valuePlaceholders = valuePlaceholders[:len(valuePlaceholders)-1]
		updateExpr := ""
		updateExprParts := make([]string, 0)
		for _, column := range columns {
			if !column.PrimaryKey {
				updateExprParts = append(updateExprParts, fmt.Sprintf("%s=VALUES(%s)", column.Name, column.Name))
			}
		}
		if len(updateExprParts) > 0 {
			updateExpr = strings.Join(updateExprParts, ", ")
		}

		if stmt == nil {
			statement := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s", tableName, getColumnNames(columns), valuePlaceholders, updateExpr)
			stmt, err = conn.Prepare(statement)
			if err != nil {
				return fmt.Errorf("error preparing upsert statement: %s", err)
			}
			defer stmt.Close()
		}

		result, err := stmt.Exec(values...)
		if err != nil {
			err = g.CreateOrUpdateTable(record)
			if err != nil {
				return fmt.Errorf("error creating/updating table: %s", err)
			}
		}
		log.Printf("Upsert result: %+v", result)
	}

	return nil
}

func (g *GoDB) TableExists(tableName string) bool {
	connPool := g.GetConnectionPool(GetReadConn)
	if connPool == nil {
		log.Println("GoDB::TableExists - error getting read connection")
		return false
	}

	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '%s')", tableName)
	row := connPool.QueryRow(query)
	var exists bool
	if err := row.Scan(&exists); err != nil {
		log.Fatalf("error checking if table exists: %s", err)
	}
	return exists
}

func (g *GoDB) getJunctionTableName(struct1 interface{}, struct2 interface{}) string {
	struct1Name := reflect.TypeOf(struct1).Elem().Name()
	struct2Name := reflect.TypeOf(struct2).Elem().Name()
	return fmt.Sprintf("%s_%s", struct1Name, struct2Name)
}

func (g *GoDB) SearchJunctionTable(struct1, struct2 interface{}, fields []string, conditions map[string]*Where) ([]map[string]interface{}, error) {
	connPool := g.GetConnectionPool(GetReadConn)
	if connPool == nil {
		return nil, fmt.Errorf("error getting read connection")
	}
	// Get the name of the junction table
	tableName := g.getJunctionTableName(struct1, struct2)

	// Build the query
	var queryParts []string
	var queryArgs []interface{}
	for columnName, where := range conditions {
		queryParts = append(queryParts, fmt.Sprintf("%s %s ?", columnName, where.Operator))
		queryArgs = append(queryArgs, where.Value)
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(fields, ", "), tableName, strings.Join(queryParts, " AND "))

	// Execute the query
	rows, err := connPool.Query(query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %s", err)
	}
	defer rows.Close()

	// Get the column names from the rows
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting column names: %s", err)
	}

	// Create a slice of maps to store the results
	results := make([]map[string]interface{}, 0)

	// Loop over the rows and populate the results slice
	for rows.Next() {
		// Create a slice to hold the row values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row values into the slice
		err := rows.Scan(values...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %s", err)
		}

		// Create a map to hold the column names and values
		rowMap := make(map[string]interface{})
		for i, column := range columns {
			value := *(values[i].(*interface{}))
			rowMap[column] = value
		}

		results = append(results, rowMap)
	}

	// Check for any errors encountered while iterating over the rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %s", err)
	}

	return results, nil
}

func (g *GoDB) Search(record interface{}, conditions map[string]*Where) ([]map[string]interface{}, error) { // TODO: Add support for ORDER BY and LIMIT clauses and write a function that also returns the values for Secondary Keys from other tables
	connPool := g.GetConnectionPool(GetReadConn)
	if connPool == nil {
		return nil, fmt.Errorf("error getting read connection")
	}

	// Get the name of the struct
	structName := reflect.TypeOf(record).Elem().Name()

	// Get the plural form of the struct name
	tableName := Pluralize(strings.ToLower(structName))

	// Check if the table exists
	if !g.TableExists(tableName) {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Construct the SELECT query
	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	var values []interface{}
	// Construct the WHERE clause for the query
	if len(conditions) > 0 {
		whereClause := " WHERE "
		for columnName, where := range conditions {
			whereClause += fmt.Sprintf("%s %s ? AND ", columnName, where.Operator)
			values = append(values, where.Value)
		}
		whereClause = strings.TrimSuffix(whereClause, "AND ")
		query += whereClause
		// Append values to query arguments
	}

	// Execute the query and scan the rows into instances of the struct
	rows, err := connPool.Query(query, values...)
	if err != nil {
		return nil, fmt.Errorf("error querying table: %s", err)
	}
	defer rows.Close()

	// Get the column names from the rows
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting column names: %s", err)
	}

	// Construct a slice of maps to be returned
	instances := make([]map[string]interface{}, 0)
	for rows.Next() {
		// Construct a map to hold the column values for this row
		rowData := make(map[string]interface{})

		// Scan the values from the row into the map
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("error scanning row: %s", err)
		}
		for i, value := range values {
			if value != nil {
				// Add the column name and value to the map
				rowData[columns[i]] = *value.(*interface{})
			}
		}

		instances = append(instances, rowData)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %s", err)
	}

	return instances, nil
}

func (g *GoDB) UpsertJunctionTable(primaryRecord interface{}, relatedRecords []interface{}, additionalColumns [][]FieldValue) error {
	// Get the names of the structs
	primaryStructName := reflect.TypeOf(primaryRecord).Elem().Name()
	relatedStructName := reflect.TypeOf(relatedRecords[0]).Elem().Name()

	// Construct the name of the junction table
	junctionTableName := fmt.Sprintf("%s_%s", primaryStructName, relatedStructName)

	// Create or update the junction table
	junctionColumns := make([]Field, 0)
	primaryColumns, _, err := getKeyColumns(primaryRecord)
	if err != nil {
		return fmt.Errorf("error getting primary key columns for %s: %s", primaryStructName, err)
	}
	primaryValues := make([]interface{}, 0, len(primaryColumns))
	for _, col := range primaryColumns {
		primaryValues = append(primaryValues, reflect.ValueOf(primaryRecord).Elem().FieldByName(col.Name).Interface())
	}
	junctionColumns = append(junctionColumns, primaryColumns...)
	var relatedColumns []Field
	for _, record := range relatedRecords {
		relatedColumns, _, _ = getKeyColumns(record)
		junctionColumns = append(junctionColumns, relatedColumns...)
		break
	}

	if len(additionalColumns) > 0 {
		recordFields := additionalColumns[0]
		if len(recordFields) > 0 {
			for _, col := range recordFields {
				junctionColumns = append(junctionColumns, *col.Field)
			}
		}
	}

	// Upsert each related record in the junction table
	allRecords := make([][]interface{}, 0, len(relatedRecords))
	for recordIndex, relatedRecord := range relatedRecords {
		additionalRecord := additionalColumns[recordIndex]
		columnValues := make([]interface{}, 0)

		// Add the primary record's key values to the column values
		columnValues = append(columnValues, primaryValues...)

		for _, col := range relatedColumns {
			columnValues = append(columnValues, reflect.ValueOf(relatedRecord).Elem().FieldByName(col.Name).Interface())
		}
		for _, col := range additionalRecord {
			columnValues = append(columnValues, col.Value)
		}

		allRecords = append(allRecords, columnValues)

	}

	err = g.UpsertTable(junctionTableName, junctionColumns, allRecords) // TODO:: Adjust this to return a map records to errors

	if err != nil {
		return fmt.Errorf("error upserting junction record: %s", err)
	}

	return nil
}

func getKeyColumns(record interface{}) ([]Field, []Field, error) {
	var primaryKeyColumns []Field
	var secondaryKeyColumns []Field

	// Get the type of the record
	recordType := reflect.TypeOf(record).Elem()

	// Iterate over the fields of the record
	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)

		// Check if the field is tagged with GoDBKey
		if keyType, ok := field.Tag.Lookup("GoDBKey"); ok {
			columnName := strings.ToLower(field.Name)
			columnType := sqlType(field.Type)

			// Determine if the key is a primary key or secondary key
			if keyType == "primary" {
				primaryKeyColumns = append(primaryKeyColumns, Field{Name: columnName, Type: columnType, PrimaryKey: true})
			} else if keyType == "secondary" {
				secondaryKeyColumns = append(secondaryKeyColumns, Field{Name: columnName, Type: columnType, SecondaryKey: true})
			}
		}
	}

	// Check if primary key columns exist
	if len(primaryKeyColumns) == 0 && len(secondaryKeyColumns) == 0 {
		return nil, nil, fmt.Errorf("no key columns found for struct")
	}

	return primaryKeyColumns, secondaryKeyColumns, nil
}

func (g *GoDB) GetTableColumns(tableName string) ([]string, error) {
	connPool := g.GetConnectionPool(GetReadConn)
	if connPool == nil {
		return nil, fmt.Errorf("error getting read connection")
	}
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", tableName)
	rows, err := connPool.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

func stringInSlice(s string, slice []string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func (g *GoDB) UpsertTable(tableName string, columns []Field, records [][]interface{}) error {
	connPool := g.GetConnectionPool(GetWriteConn)
	conn, err := connPool.getConn()
	if err != nil {
		return err
	}

	// Check if the table exists
	if !g.TableExists(tableName) {
		// Create the table if it doesn't exist
		err := g.CreateOrUpdateTableRaw(tableName, columns)
		if err != nil {
			return fmt.Errorf("error creating table: %s", err)
		}
	}

	// Prepare the statement for upserting the records
	valuePlaceholders := strings.Repeat("(?),", len(columns))
	valuePlaceholders = valuePlaceholders[:len(valuePlaceholders)-1]
	updateExpr := ""
	updateExprParts := make([]string, 0)
	for _, column := range columns {
		if !column.PrimaryKey && !column.SecondaryKey {
			updateExprParts = append(updateExprParts, fmt.Sprintf("%s=VALUES(%s)", column.Name, column.Name))
		}
	}
	if len(updateExprParts) > 0 {
		updateExpr = strings.Join(updateExprParts, ", ")
	}

	statement := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s", tableName, getColumnNames(columns), valuePlaceholders, updateExpr)
	stmt, err := conn.Prepare(statement)
	if err != nil {
		return fmt.Errorf("error preparing upsert statement: %s", err)
	}
	defer stmt.Close()

	// Attempt to upsert the records, retrying with fewer records on error
	for batchSize := len(records); batchSize >= 1; batchSize /= 2 {
		for i := 0; i < len(records); i += batchSize {
			j := i + batchSize
			if j > len(records) {
				j = len(records)
			}
			batch := records[i:j]

			// Convert the batch to a flat list of values
			values := make([]interface{}, 0, len(batch)*len(columns))
			for _, record := range batch {
				if len(record) != len(columns) {
					return fmt.Errorf("error upserting record: invalid number of values")
				}
				values = append(values, record...)
			}

			// Execute the upsert statement with the current batch of records
			_, err = stmt.Exec(values...)
			if err != nil {
				if batchSize == 1 {
					return fmt.Errorf("error upserting record: %s", err)
				}
				// Try again with a smaller batch size
				continue
			}
		}
	}

	return nil
}

// Helper function to get the column names from a slice of fields
func getColumnNames(columns []Field) string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return strings.Join(names, ", ")
}

/* Unused functions

func buildInsertOrUpdateQuery(record interface{}) (string, string, string) {
	var columns []string
	var values []string

	// Loop over the fields in the struct and build the column and value lists
	elem := reflect.ValueOf(record).Elem()
	typeOfT := elem.Type()
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if field.CanInterface() {
			columnName := strings.ToLower(typeOfT.Field(i).Name)
			columns = append(columns, columnName)
			values = append(values, "?")
		}
	}

	// Build the column and value strings for the insert query
	columnsStr := strings.Join(columns, ", ")
	valuesStr := strings.Join(values, ", ")

	// Build the update expression for the update query
	var updateExprStrs []string
	for _, col := range columns {
		updateExprStrs = append(updateExprStrs, fmt.Sprintf("%s = ?", col))
	}
	updateExprStr := strings.Join(updateExprStrs, ", ")

	return columnsStr, valuesStr, updateExprStr
}

func getFieldsFromStruct(record interface{}) ([]Field, error) {
	var fields []Field

	// Loop over the fields in the struct and build the column and value lists
	elem := reflect.ValueOf(record).Elem()
	typeOfT := elem.Type()
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if field.CanInterface() {
			f := Field{
				Name: strings.ToLower(typeOfT.Field(i).Name),
				Type: sqlType(field.Type()),
			}
			key := typeOfT.Field(i).Tag.Get("GoDBKey")
			if key == "primary" {
				f.PrimaryKey = true
			} else if key == "secondary" {
				f.SecondaryKey = true
			}
			fields = append(fields, f)
		}
	}

	if fields == nil {
		return nil, fmt.Errorf("no fields found in struct")
	}

	return fields, nil
}

*/
