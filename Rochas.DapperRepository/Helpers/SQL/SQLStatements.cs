using System;

namespace Rochas.DapperRepository.Helpers.SQL
{
    public static class SQLStatements
    {
        public static string SQL_Action_ColumnAlias = "AS {0} ";
        public static string SQL_Action_CountAggregation = "COUNT({0}.{1}) AS {2}, ";
        public static string SQL_Action_Create = "INSERT INTO {0} ({1}) VALUES ({2}) ";
        public static string SQL_Action_Delete = "DELETE FROM {0} WHERE {1}";
        public static string SQL_Action_Edit = "UPDATE {0} SET {1} WHERE {2}";
        public static string SQL_Action_ExecuteProcedure = "EXEC {0};";
        public static string SQL_Action_ExecuteProcedure_MySQL = "CALL {0}";
        public static string SQL_Action_GetLastId = "SELECT @@IDENTITY ";
        public static string SQL_Action_Group = "GROUP BY {0}";
        public static string SQL_Action_LimitResult = "TOP {0}";
        public static string SQL_Action_LimitResult_MySQL = "LIMIT {0}";
        public static string SQL_Action_MaximumAggregation = "MAX({0}.{1}) AS {2}, ";
        public static string SQL_Action_MinimumAggregation = "MIN({0}.{1}) AS {2}, ";
        public static string SQL_Action_MultipleFilter = "IN ({0})";
        public static string SQL_Action_OrderResult = "ORDER BY {0} {1}";
        public static string SQL_Action_Query = "SELECT {0} FROM {1} {2} WHERE {3} {4} {5} {6}";
        public static string SQL_Action_RangeFilter = "BETWEEN {0} AND {1}";
        public static string SQL_Action_RelationateMandatorily = "INNER JOIN {0} ON {1} = {2}";
        public static string SQL_Action_RelationateOptionally = "LEFT JOIN {0} ON {1} = {2}";
        public static string SQL_Action_SummaryAggregation = "SUM({0}.{1}) AS {2}, ";
        public static string SQL_ReservedWord_INSERT = "INSERT";
    }
}
