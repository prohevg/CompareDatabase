using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using FirebirdSql.Data.FirebirdClient;

namespace CompareDatabase.Comparies
{
    class MainCompare : DatabaseBase
    {
        private const string IDColumnName = "ID";
        private const string GUIDColumnName = "GUID";

        private readonly string _connectionString1;
        private readonly string _connectionString2;
        private readonly Action<string> _action;

        private readonly List<string> _exceptTable = new List<string>()
        {
            "EVENT",
            "RECORD_FILE",
            "EVENT_RECORD_FILE_LINK",
            "MARKER",
            "SCRIPT",
            "USER_HISTORY",
            "SYSTEM_LOG",
            "ALARM_TYPE"
        };

        #region public

        public MainCompare(string connectioString1, string connectionString2, Action<string> action)
        {
            if (string.IsNullOrEmpty(connectioString1))
            {
                throw new ArgumentNullException();
            }

            if (string.IsNullOrEmpty(connectionString2))
            {
                throw new ArgumentNullException();
            }

            if (action == null)
            {
                throw new ArgumentNullException();
            }

            _connectionString1 = connectioString1;
            _connectionString2 = connectionString2;
            _action = action;
        }

        public void Execute()
        {
            var tableNames1 = GetTables(_connectionString1);
            var tableNames2 = GetTables(_connectionString2);

            var tableCount1 = tableNames1.Count();
            var tableCount2 = tableNames2.Count();

            if (tableCount1 != tableCount2)
            {
                Error("Разное кол-во таблиц. В БД1: {0}, В БД2: {1}", tableCount1, tableCount2);
                return;
            }

            Ok("Одинаковое кол-во таблиц. В БД1: {0}, В БД2: {1}", tableCount1, tableCount2);

            Warn("Сравнимаем БД1 с БД2");
            CompareTables(tableNames1, _connectionString1, _connectionString2);

            Warn("Сравнимаем БД2 с БД1");
            CompareTables(tableNames1, _connectionString2,_connectionString1);
        }

        #endregion

        #region private

        private void CompareTables(IEnumerable<string> tableNames, string connectionString1, string connectionString2)
        {
            foreach (string tableName in tableNames.OrderBy(t => t))
            {
                if (_exceptTable.Contains(tableName))
                {
                    Warn("Таблица {0} исключена из сравнения", tableName);
                    continue;
                }

                var compareTable1 = new TableCompare(connectionString1, tableName);
                var compareTable2 = new TableCompare(connectionString2, tableName);

                CompareCountRow(compareTable1, compareTable2, tableName);
                CompareTable(compareTable1, compareTable2, tableName);
            }
        }

        private void CompareCountRow(TableCompare compareTable1, TableCompare compareTable2, string tableName)
        {
            var countRow1 = compareTable1.CountRows();
            var countRow2 = compareTable2.CountRows();

            if (countRow1 != countRow2)
            {
                Error("Разное кол-во строк в таблице {0}. В БД1 cтрок {1}, в БД2 строк {2}", tableName, countRow1, countRow2);
            }
            else
            {
                Ok("Одинаковое кол-во строк в таблице {0}. В БД1 cтрок {1}, в БД2 строк {2}", tableName, countRow1, countRow2);
            }
        }

        private void CompareTable(TableCompare compareTable1, TableCompare compareTable2, string tableName)
        {
            var rows1 = compareTable1.GetRows();
            var rows2 = compareTable2.GetRows();

            if (!rows1.Any())
            {
                return;
            }

            Ok("Сравниваем таблицу {0} по Guid записей", tableName);

            CompareRows(tableName, rows1, rows2);

            Ok("Сравнение таблицы {0} по Guid записей завершено", tableName);

            Ok("Сравниваем таблицу {0} по значениям записей", tableName);

            foreach (var row1 in rows1)
            {
                var guid1 = CheckGuidInRow(tableName, row1, 1, false);
                if (!guid1.HasValue)
                {
                    return;
                }

                var byGuidRows2 = GetRowsByGuid(guid1.Value, rows2, tableName, 2, false);
                if (byGuidRows2.Count() == 1)
                {
                    CompareRow(tableName, row1, byGuidRows2.First());
                }
            }

            Ok("Сравнение таблицы {0} по значениям записей завершено", tableName);
        }

        private void CompareRows(string tableName, IEnumerable<DataRow> rows1, IEnumerable<DataRow> rows2)
        {
            foreach (var row1 in rows1)
            {
                if (row1.Table.Columns.Contains(GUIDColumnName))
                {
                    var guid1 = CheckGuidInRow(tableName, row1, 1);
                    if (!guid1.HasValue)
                    {
                        continue;
                    }

                    var findRows2 = GetRowsByGuid(guid1.Value, rows2, tableName, 2);
                    if (!findRows2.Any())
                    {
                        Error("В БД2 в таблице {0} не найдена строка с guid={1}", tableName, guid1);
                        continue;
                    }

                    if (findRows2.Count() > 1)
                    {
                        Error("В БД2 в таблице {0} больше одной строки с guid={1}", tableName, guid1);
                    }
                }
            }
        }

        private void CompareRow(string tableName, DataRow row1, DataRow row2)
        {
            if (!row1.Table.Columns.Contains(GUIDColumnName))
            {
               return;
            }

            var guid1 = CheckGuidInRow(tableName, row1, 1, false);
            if (!guid1.HasValue)
            {
                return;
            }

            foreach (DataColumn column in row1.Table.Columns)
            {
                var value1 = row1[column.ColumnName];

                if (!row2.Table.Columns.Contains(column.ColumnName))
                {
                    Error("В таблице {0} в БД2 не стоблца: {1}", tableName, column.ColumnName);
                    continue;
                }

                var value2 = row2[column.ColumnName];

                var mess = string.Format("В таблице {0} в строке с GUID {1} разные значения в столбце {2}. В БД1: {3}, В БД2: {4}", tableName, guid1, column.ColumnName, value1, value2);

                if ((value1 == null && value2 != null)
                    ||
                    (value1 != null && value2 == null))
                {
                    if (column.ColumnName == IDColumnName)
                    {
                        Warn(mess);
                    }
                    else
                    {
                        Error(mess);
                    }
                    return;
                }

                if (value1 == null && value2 == null)
                {
                    return;
                }

                if (value1 != null && !value1.ToString().ToUpper().Equals(value2.ToString().ToUpper()))
                {
                    if (column.ColumnName == IDColumnName)
                    {
                        Warn(mess);
                    }
                    else
                    {
                        Error(mess);
                    }
                }
            }
        }

        private IEnumerable<string> GetTables(string connectionString)
        {
            var result = new List<string>();
            string commandText = "select rdb$relation_name from rdb$relations where rdb$view_blr is null and (rdb$system_flag is null or rdb$system_flag = 0);";
            using (IDbConnection connection = CreateConnection(connectionString))
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

        private IEnumerable<DataRow> GetRowsByGuid(Guid guidEthalon, IEnumerable<DataRow> rows, string tableName, int bdNumber, bool isAddToLog = true)
        {
            if (!rows.Any())
            {
                return new List<DataRow>();
            }

            var firstRow = rows.FirstOrDefault();
            if (!firstRow.Table.Columns.Contains(GUIDColumnName))
            {
                return new List<DataRow>();
            }

            var result = new List<DataRow>();
            foreach (var row in rows)
            {
                var guid = CheckGuidInRow(tableName, row, bdNumber, isAddToLog);
                if (guid.HasValue && guidEthalon == guid.Value)
                {
                    result.Add(row);
                }
            }
            return result;
        }

        private Guid? CheckGuidInRow(string tableName, DataRow row, int bdNumber, bool isAddToLog = true)
        {
            if (!row.Table.Columns.Contains(GUIDColumnName))
            {
                return null;
            }

            if (row[GUIDColumnName] == null)
            {
                if (isAddToLog)
                {
                    Error("В БД{0} в таблице {1} есть поле GUID, но его значение null", bdNumber, tableName);
                }
                return null;
            }

            Guid? guid = null;
            try
            {
                guid = Guid.Parse(row[GUIDColumnName].ToString().Replace(" ", ""));
                if (guid == Guid.Empty)
                {
                    if (isAddToLog)
                    {
                        if (row.Table.Columns.Contains(IDColumnName))
                        {
                            Warn("В БД{0} в таблице {1} есть поле GUID={2}. ID записи={3}", bdNumber, tableName, guid, row[IDColumnName]);
                        }
                        else
                        {
                            Warn("В БД{0} в таблице {1} есть поле GUID={2}", bdNumber, tableName, guid);
                        }
                    }

                    return guid;
                }
            }
            catch (Exception)
            {
                if (isAddToLog)
                {
                    Error("В БД{0} в таблице {1} есть поле GUID, но его значение невозможно распарсить", bdNumber, tableName);
                }
            }

            return guid;
        }

        private void Ok(string message, params object[] param)
        {
            _action(string.Format(message, param));
        }

        private void Warn(string message, params object[] param)
        {
            _action("-----   " + string.Format(message, param));
        }

        private void Error(string message, params object[] param)
        {
            _action("!!!!!   " + string.Format(message, param));
        }

        #endregion
    }
}
