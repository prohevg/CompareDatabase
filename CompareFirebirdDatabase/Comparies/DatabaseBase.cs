using System.Data;
using FirebirdSql.Data.FirebirdClient;

namespace CompareDatabase.Comparies
{
    public class DatabaseBase
    {
        /// <summary>
        /// Создать соединенеия с БД
        /// </summary>
        /// <returns>Соединение с БД</returns>
        protected IDbConnection CreateConnection(string connectionString)
        {
            return new FbConnection(connectionString);
        }

        /// <summary>
        /// Создать команду
        /// </summary>
        /// <param name="commandText">Текст команды</param>
        /// <param name="connection">Соединения с БД</param>
        /// <param name="transaction">Транзакция, если необходима</param>
        /// <returns>Команда работы с БД</returns>
        protected IDbCommand CreateCommand(string commandText, IDbConnection connection, IDbTransaction transaction = null)
        {
            var fbConnection = connection as FbConnection;
            var fbTransaction = transaction as FbTransaction;
            return new FbCommand(commandText, fbConnection, fbTransaction);
        }
    }
}