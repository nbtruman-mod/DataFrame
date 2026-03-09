<?php
/**
 * Created by PhpStorm.
 * User: ntruman
 * Date: 06/03/2026
 * Time: 13:00
 */

namespace NEMESIS\DataFrame;

final class DataFrame
{
    /** @var  array<int,string|int> */
    private $header;
    /** @var array<int,array<int,mixed>> */
    private $rows;
    /** @var array<string,int>  */
    private $column_index = [];

    public function __construct(array $rows, array $header = [])
    {
        $this->rows = $rows;

        if (empty($header) && !empty($rows)) {
            $header = range(0, count($rows[0]) - 1);
        }
        $this->header = $header;

        $this->validate();
        $this->buildColumnIndex();
    }

    private function validate(): void
    {
        if (empty($this->rows)) {
            return;
        }

        $column_count = count($this->rows[0]);

        foreach ($this->rows as $i => $row) {
            if (count($row) !== $column_count) {
                throw new \RuntimeException("Row {$i} has incorrect column count");
            }
        }

        if (!empty($this->header) && count($this->header) !== $column_count) {
            throw new \RuntimeException("Header column count does not match row column count");
        }
    }

    private function buildColumnIndex(): void
    {
        foreach ($this->header as $i => $name) {
            if (isset($this->column_index[$name])) {
                throw new \RuntimeException("Duplicate column name: {$name}");
            }

            $this->column_index[$name] = $i;
        }
    }

    public static function fromCSVReader(\NEMESIS\CSVReader\CSVReader $reader): self
    {
        return new self($reader->getRows(), $reader->getHeader());
    }

    public function rowCount(): int
    {
        return count($this->rows);
    }

    public function columnCount(): int
    {
        return count($this->header);
    }

    public function getHeader(): array
    {
        return $this->header;
    }

    public function head(int $n = 5): self
    {
        return new self(array_slice($this->rows, 0, $n), $this->header);
    }

    private function resolveColumn($column): int
    {
        if (is_int($column)) {
            return $column;
        }

        if (!is_string($column)) {
            $type = gettype($column);
            throw new \TypeError("Arguments to resolveColumn must be of type int or string: {$type} given");
        }

        if (!isset($this->column_index[$column])) {
            throw new \RuntimeException("Unknown column: {$column}");
        }

        return $this->column_index[$column];
    }

    public function getColumn($column): array
    {
        $index = $this->resolveColumn($column);

        $values = [];

        foreach ($this->rows as $row) {
            $values[] = $row[$index];
        }

        return $values;
    }

    public function select(array $columns): self
    {
        $indexes = array_map(
          function ($c) {
              return $this->resolveColumn($c);
          },
          $columns
        );

        $new_rows = [];

        foreach ($this->rows as $row) {
            $new_row = [];

            foreach ($indexes as $i) {
                $new_row[] = $row[$i];
            }

            $new_rows[] = $new_row;
        }

        $new_header = [];

        if (!empty($this->header)) {
            foreach ($indexes as $i) {
                $new_header[] = $this->header[$i];
            }
        }

        return new self($new_rows, $new_header);
    }

    public function filter(callable $callback): self
    {
        $new_rows = [];

        foreach ($this->rows as $row) {
            $row_for_callback = array_combine($this->header, $row);

            if ($callback($row_for_callback)) {
                $new_rows[] = $row;
            }
        }

        return new self($new_rows, $this->header);
    }

    public function map(callable $callback): self
    {
        $new_rows = [];

        foreach ($this->rows as $i => $row) {
            $row_for_callback = array_combine($this->header, $row);

            $result = $callback($row_for_callback);

            if (!is_array($result)) {
                $type = gettype($result);

                throw new \RuntimeException(
                    "DataFrame::map() callback must return an array representing a row. "
                    . "Row {$i} returned type {$type}"
                );
            }

            $expected_column_count = $this->columnCount();

            if (count($result) !== $expected_column_count) {
                throw new \RuntimeException(
                    "DataFrame::map() callback returned incorrect column count on row {$i}"
                    . "Expected {$expected_column_count}, got " . count($result)
                );
            }

            if ($this->header) {
                $result = array_values($result);
            }

            $new_rows[] = $result;
        }

        return new self($new_rows, $this->header);
    }

    public function addColumn(string $name, callable $callback): self
    {
        $new_rows = [];

        foreach ($this->rows as $i => $row) {
            $row_for_callback = array_combine($this->header, $row);

            $value = $callback($row_for_callback);

            $new_row = $row;

            $new_row[] = $value;

            $new_rows[] = $new_row;
        }

        $new_header = $this->header;

        if (isset($this->column_index[$name])) {
            throw new \RuntimeException("Column '{$name}' already exists.");
        }

        $new_header[] = $name;

        return new self($new_rows, $new_header);
    }

    public function renameColumn($column, string $new_name): self
    {
        $index = $this->resolveColumn($column);

        if (isset($this->column_index[$new_name])) {
            throw new \RuntimeException("Column '{$new_name}' already exists.");
        }

        $new_header = $this->header;
        $new_header[$index] = $new_name;

        return new self($this->rows, $new_header);
    }

    public function renameColumns(array $new_names): self
    {
        if (count($new_names) !== $this->columnCount()) {
            throw new \RuntimeException(
                "renameColumns expects {$this->columnCount()} columns names, got "
                . count($new_names)
            );
        }

        if (count(array_unique($new_names)) !== count($new_names)) {
            throw new \RuntimeException("renameColumns received duplicate column names");
        }

        return new self($this->rows, array_values($new_names));
    }

    public function toArray(): array
    {
        return $this->rows;
    }

    public function toAssoc(): array
    {
        $rows = [];

        foreach($this->rows as $row) {
            $rows[] = array_combine($this->header, $row);
        }

        return $rows;
    }
}