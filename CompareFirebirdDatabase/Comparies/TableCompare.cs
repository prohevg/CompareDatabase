using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;


namespace CompareDatabase.Comparies
{
    public class TableCompare : DatabaseBase
    {
        private readonly string _connectionString;
        private readonly string _tableName;

        public TableCompare(string connectionString, string tableName)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException();
            }

            if (string.IsNullOrEmpty(tableName))
            {
                throw  new ArgumentNullException();
            }

            _connectionString = connectionString;
            _tableName = tableName;
        }

        /// <summary>
        /// Строк в таблице
        /// </summary>
        /// <returns></returns>
        public int CountRows()
        {
            string commandText = "select count(*) from " + _tableName;
            using (IDbConnection connection = CreateConnection(_connectionString))
            {
                connection.Open();

                using (IDbCommand command = CreateCommand(commandText, connection))
                {
                    var reader = command.ExecuteReader();
                    return reader.Read() ? reader.GetInt32(0) : 0;
                }
            }
        }

        /// <summary>
        /// Строк в таблице
        /// </summary>
        /// <returns></returns>
        public IEnumerable<DataRow> GetRows()
        {
            var columns = GetColumns();
            var sb = new StringBuilder();

            int i = 0;
            foreach (string column in columns)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                sb.Append(column);
                i++;
            }

            var result = new List<DataRow>();
            string commandText = "select " + sb + " from " + _tableName;
            using (IDbConnection connection = CreateConnection(_connectionString))
            {
                connection.Open();

                using (IDbCommand command = CreateCommand(commandText, connection))
                {
                    DataTable dt = new DataTable();
                    dt.Load(command.ExecuteReader());
                    foreach (DataRow row in dt.Rows)
                    {
                        result.Add(row);
                    }
                }
            }

            return result;
        }

        private IEnumerable<string> GetColumns()
        {
            var result = new List<string>();
            string commandText = "select f.rdb$field_name from rdb$relation_fields f where upper(rdb$relation_name) = '" + _tableName + "'";
            using (IDbConnection connection = CreateConnection(_connectionString))
            {
                connection.Open();

                using (IDbCommand command = CreateCommand(commandText, connection))
                {
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        result.Add(reader.GetString(0).Replace(" ", ""));
                    }
                }
            }
            return result;
        }
    }
}
