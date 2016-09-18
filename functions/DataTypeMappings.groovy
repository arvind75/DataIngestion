package genddl

/**
 * Holds DataType Mappings: { SAP_BW => Hive, Oracle_Apps => Hive }
 */
class DataTypeMappings {

  def dataTypeMappings() {
    [
      NUMBER:    'int',
      CHAR:      'string',
      TIMS:      'string',
      DATS:      'date',
      CUKY:      'string',
      NUMC:      'integer',
      UNIT:      'string',
      DATE:      'date',
      CHARACTER: 'string',
      VARCHAR:   'string',
      VARCHAR2:  'string',
      NVARCHAR:  'string',
      INTEGER:   'integer',
      DECIMAL:   'decimal',
      DEC:       'decimal',
      NUMERIC:   'decimal',
      CURR:      'decimal',
      QUANT:     'decimal',
      QUAN:      'decimal',
      INT4:      'integer',
      TIMESTAMP: 'timestamp',
      INT:	 'integer',
      NCHAR:	 'string',
      SMALLINT:	 'integer',
      DATETIME:	 'timestamp',
      TINYINT:	 'integer'
        ];
  }
}
