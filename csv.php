<?php

/**
 * Agregation functions
 *
 * @return Callable
 */

class CsvAgreggation {

    public static function last($header) {
        return [function ($acc, $row) use ($header) {
            return $row[$header];
        },null];
    }

    public static function sum($header) {
        return [function ($acc, $row) use ($header) {
            return $acc + $row[$header];
        },0];
    }

    public static function count() {
        return [function ($acc,$row) {
            return $acc + 1;
        }, 0];
    }

    public static function countNotEmpty($header) {
        return [function ($acc,$row) use ($header) {
            return $acc + (empty($row[$header]) ? 0 : 1);
        },0];
    }

    public static function max($header) {
        return [ function ($acc,$row,$i,$all) use ($header) {
            if ($i === 0) {
                $acc = $row[$header];
            } else if (is_string($row[$header]) && strcmp($acc,$row[$header]) < 0) {
                $acc = $row[$header];
            } else if ( $acc < $row[$header]) {
                $acc = $row[$header];
            }
            return $acc;
        },null];
    }

    public static function min($header) {
        return [ function ($acc,$row,$i,$all) use ($header) {
            if ($i === 0) {
                $acc = $row[$header];
            } else if (is_string($row[$header]) && 0 < strcmp($acc,$row[$header]) ) {
                $acc = $row[$header];
            } else if ( $row[$header] < $acc) {
                $acc = $row[$header];
            }
            return $acc;
        },null];
    }
};

class Csv {

    public $headers = array();
    public $rows = array();
    public $delimiter = ';';
    public $dateTimeFormat = 'Y-m-d H:i:s';

    /**
     * Erases actual content and imports from a table.
     * 
     * 
     * @param &$table Array Input associative table data.
     * @param $headers Array Optional. Each element could be:
     *      
     *      - Boolean. Selects this column if true, does not select it if false.
     *                 The key of this element will be the source and destination column name.
     *      - Symbol. The name of the field to import. The key of this element will be the
     *                destination column name if it is a string. Otherwise, the provided value
     *                will be also the destination value.
     *      - Array. The key of this element will be the destination column name.
     *               First element contains the name of the column where it is going to import
     *               (ie, destination name).
     *               If more elements are provided, they should be functions as has the same
     *               effect as described below. They will be chained.
     *      - Function. The key of this element will be the destination column name.
     *                  A function with signature Value($value,$row,&$table) which returns
     *                  value of the destination.
     * 
     * @example Example 1:
     * 
     * $data = [
     *      [
     *          'column1' => 1,
     *          'column2' => 2,
     *          'column3' => 3,
     *      ],
     *      [
     *          'column1' => 4,
     *          'column2' => 5,
     *          'column3' => 6,
     *      ],
     * ];
     * 
     * $headers = [
     *      'column1',
     *      'column2' => false,
     *      'again_column1' => 'column1',
     *      'three_times_column1_plus_column2' => [
     *          'column_1',
     *          function ($value,$row,$table) {
     *              return 3*$value;
     *          },
     *          function ($value,$row,$table) {
     *              return $value + $row['column_2'];
     *          }
     *      ],
     * 
     *      'column3' => function ($value,$row,$table) {
     *          return $value + 1;
     *      },
     *      
     * ];
     * 
     * Then, if we do:
     * 
     * $result = new Csv();
     * $result->importTable($data,$headers);
     * var_dump($result);
     * 
     * we obatain:
     * 
     * object(Csv)#4 (3) {
     *  ["headers"]=>
     *   array(4) {
     *       [0]=>
     *       string(7) "column1"
     *       [1]=>
     *       string(13) "again_column1"
     *       [2]=>
     *       string(32) "three_times_column1_plus_column2"
     *       [3]=>
     *       string(7) "column3"
     *   }
     *   ["rows"]=>
     *   array(2) {
     *       [0]=>
     *       array(4) {
     *       ["column1"]=>
     *       int(1)
     *       ["again_column1"]=>
     *       int(1)
     *       ["three_times_column1_plus_column2"]=>
     *       int(5)
     *       ["column3"]=>
     *       int(4)
     *       }
     *       [1]=>
     *       array(4) {
     *       ["column1"]=>
     *       int(4)
     *       ["again_column1"]=>
     *       int(4)
     *       ["three_times_column1_plus_column2"]=>
     *       int(17)
     *       ["column3"]=>
     *       int(7)
     *       }
     *   }
     *   ["delimiter"]=>
     *   string(1) ";"
     *   }
     * 
     * @return Csv
     */

    function importTable(&$data, $headers = null) {
        
        if (!is_array($data)) {
            // TODO throw error
            return $this;
        }

        $this->headers = [];
        $this->rows = [];

        // build headers if proceed
        if ($headers !== null) {
            foreach($headers as $header_dst => $value) {
                if (!is_bool($value) || $value) {
                    $this->headers[] = $header_dst;
                }
            }
        }

        // no rows
        if (count($data) <= 0) {
            return $this;
        }

        // default headers
        if ($headers === null) {
            foreach ($data[0] as $field => $value) {
                $headers[$field] = true;
                $this->headers[] = $field;
            }
        }

        // get data
        foreach ($data as $i => $row) {
            $row_dst = [];

            foreach ($headers as $header_dst => $header_value) {

                // bool case
                if (is_bool($header_value) && $header_value) {

                    $row_dst[$header_dst] = $row[$header_dst];

                // string case renaming
                } else if (is_string($header_value) && is_string($header_dst)) {
                
                    $row_dst[$header_dst] = $row[$header_value];
                    
                // string case not renaming
                } else if (is_string($header_value) && !is_string($header_dst)) {

                    $row_dst[$header_value] = $row[$header_value];
                
                // function case
                } else if (is_callable($header_value)) {

                    $row_dst[$header_dst] = $header_value($row[$header_dst],$row,$data);

                // array case
                } else if (is_array($header_value) && count($header_value) > 0) {

                    $header_src = $header_value[0];
                    $value = $row[$header_src];
                    for ($j = 1; $j < count($header_value); $j++) {
                        $f = $header_value[$j];
                        if (is_callable($f)) {
                            $value = $f($value,$row,$data);
                        }
                    }
                    $row_dst[$header_dst] = $value;
                }
            }
            $this->rows[] = $row_dst;
        }


        return $this;
    }

    /**
     * Constructor
     *
     * @param $filename String Optional. Reads file.
     * @param $delimiter String Optional. Sets the field delimiter.
     * 
     * @return Void
     */

    function __construct($filename = null, $delimiter = ';') {
        $this->delimiter = $delimiter;
        if (is_string($filename) && !empty($filename)) {
            $this->read($filename);
        }
    }

    /**
     * Sort rows by a comparator function
     *
     * @param $f Callable Comparator function
     *
     * @return Csv
     */

    public function sort($f) {

        usort($this->rows,$f);

        return $this;
    }

    /**
     * Do a query
     *
     * @param $select Array Associative arrays where keys are the name of columns to get and values are one of:
     *      - boolean: determines if get this column or not
     *      - string: determine the column name to get. If key is not a string, it will be also the destination field name.
     *      - function: value($acc,$row,$i,$all_acc)
     *      - array with one argument: first argument is a function. Null will by used as first argument
     *      - array with two argument: first one function, second initial value
     * The value will be as a reduce method over the lines than groupBy returns the same identifier.
     *
     * @param $where Callable Function with signatere Bool($row,$index,$self) that determines if row is used or not for computation. If true recived, all rows will be used.
     * @param $groupBy Callable Function with signature Symbol($row,$index,$self) that returns an identifier for what's unique. If null, no aggrupation will be done.
     *
     * @return Csv
     */

    public function query($select, $where = true, $groupBy = null) {

        if (is_bool($where) && $where) {
            $where = function () { return true; };
        }

        if (is_null($groupBy)) {
            $groupBy = function ($row,$i) { return $i; };
        }

        $parsed_selects = [];
        foreach($select as $select_key => $value) {
            if (is_bool($value) && !$value) {
                // do nothing
            } else if (is_bool($value) && $value) {
                $parsed_selects[] = [
                    'name' => $select_key,
                    'f' => function ($acc, $row) use ($select_key) {
                        return $row[$select_key];
                    },
                    'init' => null,
                ];
            } else if (is_string($value) && is_string($select_key)) {
                $parsed_selects[] = [
                    'name' => $select_key,
                    'f' => function ($acc, $row) use ($value) {
                        return $row[$value];
                    },
                    'init' => null,
                ];
            } else if (is_string($value) && !is_string($select_key)) {
                $parsed_selects[] = [
                    'name' => $value,
                    'f' => function ($acc, $row) use ($value) {
                        return $row[$value];
                    },
                    'init' => null,
                ];
            } else if (is_callable($value)) {
                $parsed_selects[] = [
                        'name' => $select_key,
                        'f' => $value,
                        'init' => null,
                    ];
            } else if (is_array($value) && count($value) <= 0) {
                // do nothing
            } else if (is_array($value) && count($value) <= 1) {
                $parsed_selects[] = [
                    'name' => $select_key,
                    'f' => $value[0],
                    'init' => null,
                ];
            } else if (is_array($value) && count($value) <= 2) {
                $parsed_selects[] = [
                    'name' => $select_key,
                    'f' => $value[0],
                    'init' => $value[1],
                ];
            } else {
                // do nothing
            }
        }
        
        $headers = array_column($parsed_selects,'name');

        $groupCounters = array();

        $groupping = $this->reduce(function ($acc,$row,$index,$self) use ($select, $parsed_selects, $groupBy, $groupCounters) {
            $key = $groupBy($row,$index,$self);

            if (!isset($groupCounters[$key])) {
                $groupCounters[$key] = 0;
            }

            if (!isset($acc[$key])) {
                $acc[$key] = array();
                $groupCounters[$key] = 0;
                foreach($parsed_selects as $parsed_select) {
                    $acc[$key][$parsed_select['name']] = $parsed_select['init'];
                }
            }

            foreach ($parsed_selects as $parsed_select) {
                $field_dst = $parsed_select['name'];
                $f = $parsed_select['f'];

                $acc[$key][$field_dst] = $f($acc[$key][$field_dst], $row, $groupCounters[$key] , $acc[$key]);
            }

            $groupCounters[$key]++;

            return $acc;

        }, array());
        
        $result = new Csv();
        $result->headers = $headers;

        foreach($groupping as $groupRow) {
            $row = array();
            foreach ($headers as $header) {
                $row[$header] = $groupRow[$header];
            }
            $result->rows[] = $row;
        }

        return $result;
    }

    /**
     * Get a column in array
     *
     * @param $header String Name of the column.
     *
     * @return Array
     */

    public function getColumn($name) {
        return $this->rowMap(function($curr,$i,$self) use ($name) {
            return $curr[$name];
        });
    }

    /**
     * Determine if a column name exists.
     *
     * @param $header String Name of the column.
     *
     * @return boolean
     */

    public function hasColumn($name) {
        return in_array($name, $this->headers);
    }


    /**
     * Row Map
     *
     * @param $f Callable
     *
     * @return Object
     */

    public function rowMap($f) {
        return $this->reduce( function ($acc, $curr, $i, $self) use ($f) {
            $acc[] = $f($curr,$i,$self);
            return $acc;
        },array());
    }

    /**
     * Adds a column
     *
     * @param $header String Name of the new column.
     * @param $column Array Column to be added.
     *
     * @return Csv
     */

    public function addColumn($name, &$column) {

        $this->headers[] = $name;

        for ($i = 0; $i < count($this->rows); $i++) {
            $this->rows[$i][$name] = $column[$i];
        }

        return $this;
    }

    /**
     * Reduce
     *
     * @param $f Callable Function to be call in each iteration
     * @param $init Initial value. If null, first element will be used instead.
     *
     * @return Object
     */

    public function reduce(Callable $f, $init = null) {
        $i = 0;
        if ($init === null) {
            $init = $this->rows[0];
            $i++;
        }

        while ($i < count($this->rows)) {
            $init = $f($init,$this->rows[$i],$i,$this);
            $i++;
        }

        return $init;
    }

    /**
     * Parse strings to floats
     *
     * @param $fields Array Array of fields to be parsed.
     *
     * @return Csv
     */
    public function parseNumbers($fields) {

        if ( is_string($fields) ) {
            return $this->parseNumbers(array($fields));
        } else if (!is_array($fields)) {
            return $this;
        }

        $successGlobal = true;
        foreach ($fields as $field) {
            $success = in_array($field,$this->headers);
            $successGlobal = $successGlobal && $success;
            if ($success) {
                foreach ($this->rows as &$row) {
                    if (isset($row[$field])) {
                        $row[$field] = floatval(preg_replace('/,/','',$row[$field]));
                    }
                }
            }
        }

        return $this;
    }

    /**
     * Parse strings to dates
     *
     * @param $fields Array Array of fields to be parsed.
     * @param $format String Format fields are.
     *
     * @return Csv
     */

    public function parseDates($fields, $format = "Y-m-d H:i:s") {

        if ( is_string($fields) ) {
            return $this->parseDates(array($fields), $format);
        } else if (!is_array($fields)) {
            return $this;
        }

        $successGlobal = true;
        foreach ($fields as $field) {
            $success = isset($this->header[$field]);
            $successGlobal = $successGlobal && $success;
            foreach ($this->rows as &$row) {
                if (isset($row[$field])) {
                    $row[$field] = DateTime::createFromFormat($format,$row[$field]);
                }
            }
        }

        return $this;
    }

    /**
     * Write csv to string
     *
     * @param $filename string
     * @param $options Array Optional.
     *
     *      - null: Default ''. Value outputed for null values.
     *      - dateTimeFormat: Default $this->dateTimeFormat. DateTime formats.
     *      - delimiter: Default $this->delimiter. Delimiter.
     *
     * @return String;
     */
    public function toString($options = []) {

        $options['null'] = isset($options['null']) ? $options['null'] : '';
        $options['dateTimeFormat'] = isset($options['dateTimeFormat']) ? $options['dateTimeFormat'] : $this->dateTimeFormat;
        $options['delimiter'] = isset($options['delimiter']) ? $options['delimiter'] : $this->delimiter;

        $content = '';
        $content .= implode($options['delimiter'],$this->headers)."\n";

        foreach ($this->rows as $row) {
            $values = array();
            foreach ($this->headers as $header) {

                $value = $row[$header];

                if (is_a($value, 'DateTime')) {
                    $values[] = $value->format($options['dateTimeFormat']);
                } else if ($value === null) {
                    $values[] = $options['null'];
                } else {
                    $values[] = $value;
                }
            }
            $content .= implode($options['delimiter'],$values)."\n";
        }

        return $content;
    }

    /**
     * Write csv to filename
     *
     * @param $filename string
     * @param $options Array Optional.
     *
     *      - null: Default ''. Value outputed for null values.
     *      - dateTimeFormat: Default $this->dateTimeFormat. DateTime formats.
     *      - delimiter: Default $this->delimiter. Delimiter.
     *
     * @return Csv
     */
    public function write($filename, $options = []) {

        $content = $this->toString($options);

        file_put_contents($filename,$content);

        return $this;
    }


    /**
     * Read Csv from filename
     *
     * @param $filename
     *
     * @return Csv
    */

    public function read($filename) {
        $content = file_get_contents($filename);
        $lines = explode("\n",$content);

        // read and parse headers
        $headers_str = $lines[0];
        $headers = explode($this->delimiter,$headers_str);
        foreach ($headers as $j=>$header) {
            $headers[$j] = strtolower(preg_replace("/[^A-Za-z0-9 _-]/", '', $header));
        }


        // read and parse lines
        array_shift($lines);

        $this->headers = $headers;
        $this->rows = array();
        foreach ($lines as $line) {
            if (!empty($line) ) {
                $line_data = explode(';',$line);

                $parsed_line_data = array();
                foreach( $headers as $j => $header) {
                    $parsed_line_data[$header] = preg_replace('/[\r\n]/','',$line_data[$j]);
                }

                $this->rows[] = $parsed_line_data;
            }
        }

        return $this;
    }

}