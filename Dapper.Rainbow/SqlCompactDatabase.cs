using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Dapper.Rainbow
{
    public abstract class SqlCompactDatabase<TDatabase> : SqlDatabase<TDatabase>, IDisposable where TDatabase : SqlDatabase<TDatabase>, new()
    {
        public class SqlCompactTable<T> : SqlDatabaseTable<T>
        {
            public SqlCompactTable(SqlDatabase<TDatabase> database, string likelyTableName)
                : base(database, likelyTableName)
            {
            }

            /// <summary>
            /// Insert a row into the db
            /// </summary>
            /// <param name="data">Either DynamicParameters or an anonymous type or concrete type</param>
            /// <returns></returns>
            public override int? Insert(dynamic data)
            {
                var o = (object)data;
                List<string> paramNames = GetParamNames(o);
                paramNames.Remove("Id");

                string cols = string.Join(",", paramNames.Select(QuoteIdentifier));
                string cols_params = string.Join(",", paramNames.Select(p => "@" + p));

                var sql = "insert " + TableName + " (" + cols + ") values (" + cols_params + ")";
                if (database.Execute(sql, o) != 1)
                {
                    return null;
                }

                return (int)database.Query<decimal>("SELECT @@IDENTITY AS LastInsertedId").Single();
            }

            protected override string PrependNoCount(string sql)
            {
                return sql;
            }

            protected virtual string AppendIdentitySelect(string sql)
            {
                return sql;
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
            return CreateTableConstructor(typeof(SqlCompactTable<>));
        }
    }
}
