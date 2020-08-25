using System;
using System.Collections.Generic;
using System.Text;

namespace Rochas.DapperRepository.Helpers.SQL
{
    public static class SqlOperator
    {
        #region Declarations

        public const string Equal = " = ";
        public const string Different = " <> ";
        public const string Contains = " LIKE '%{0}%' ";
        public const string And = " AND ";
        public const string Or = " OR ";
        public const string Major = " > ";
        public const string MajorOrEqual = " >= ";
        public const string Less = " < ";
        public const string LessOrEqual = " <= ";
        public const string IsNull = " IS NULL ";
        public const string In = " IN ({0})";
        public const string Between = "BETWEEN {0} AND {1}";
        public const string Not = " NOT ";

        #endregion
    }
}
