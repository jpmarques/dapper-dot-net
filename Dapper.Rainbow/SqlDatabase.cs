using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Dapper.Rainbow
{
    public abstract class SqlDatabase<TDatabase> : Database<TDatabase>, IDisposable where TDatabase : Database<TDatabase>, new()
    {
        public class SqlDatabaseTable<T> : Table<T>
        {
            public SqlDatabaseTable(Database<TDatabase> database, string likelyTableName)
                : base(database, likelyTableName)
            {
            }

            protected override string QuoteIdentifier(string name)
            {
                return "[" + name.Replace("]", "]]") + "]";
            }
        }

        public static TDatabase Init(DbConnection connection)
        {
            TDatabase db = new TDatabase();
            db.InitDatabase(connection, 0);
            return db;
        }        

        internal override Action<TDatabase> CreateTableConstructorForTable()
        {
            return CreateTableConstructor(typeof(SqlDatabaseTable<>));
        }
    }
}
