using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Rochas.DapperRepository.Annotations;
using Rochas.DapperRepository.Enums;
using Rochas.DapperRepository.Exceptions;
using Rochas.DapperRepository.Helpers.SQL;

namespace Rochas.DapperRepository.Helpers
{
    public static class EntitySqlParser
    {
        #region Public Methods

        /// <summary>
        /// Parse entity model object instance to SQL ANSI CRUD statements
        /// </summary>
        /// <param name="entity">Entity model class reference</param>
        /// <param name="persistenceAction">Persistence action enum (Get, List, Create, Edit, Delete)</param>
        /// <param name="filterEntity">Filter entity model class reference</param>
        /// <param name="recordLimit">Result records limit</param>
        /// <param name="onlyListableAttributes">Flag to return only attributes marked as listable</param>
        /// <param name="showAttributes">Comma separeted list of custom object attributes to show</param>
        /// <param name="rangeValues"></param>
        /// <param name="groupAttributes">List of object attributes to group results</param>
        /// <param name="orderAttributes">List of object attributes to sort results</param>
        /// <param name="orderDescending">Flag to return ordering with descending order</param>
        /// <param name="readUncommited">Flag to return uncommited transaction queries statements (NOLOCK)</param>
        /// <returns></returns>
        public static string ParseEntity(object entity, PersistenceAction persistenceAction, object filterEntity = null, int recordLimit = 0, bool onlyListableAttributes = false, string showAttributes = null, IDictionary<string, double[]> rangeValues = null, string groupAttributes = null, string orderAttributes = null, bool orderDescending = false, bool readUncommited = false)
        {
            try
            {
                string sqlInstruction;
                string[] displayAttributes = new string[0];
                Dictionary<object, object> attributeColumnRelation;

                var entityType = entity.GetType();
                var entityProps = entityType.GetProperties();

                // Model validation
                if (!EntityReflector.VerifyTableAnnotation(entityType))
                    throw new InvalidOperationException("Entity table annotation not found. Please review model definition.");

                if (EntityReflector.GetKeyColumn(entityProps) == null)
                    throw new KeyNotFoundException("Entity key column annotation not found. Please review model definition.");
                //

                if (onlyListableAttributes)
                    EntityReflector.ValidateListableAttributes(entityProps, showAttributes, out displayAttributes);

                sqlInstruction = GetSqlInstruction(entity, entityType, entityProps, persistenceAction, 
                                                   filterEntity, displayAttributes, rangeValues, groupAttributes, readUncommited);

                sqlInstruction = string.Format(sqlInstruction, recordLimit > 0
                               ? string.Format(SQLStatements.SQL_Action_LimitResult_MySQL, recordLimit)
                               : string.Empty, "{0}", "{1}");

                attributeColumnRelation = EntityReflector.GetPropertiesValueList(entity, entityType, entityProps, persistenceAction);

                if (!string.IsNullOrEmpty(groupAttributes))
                    ParseGroupingAttributes(attributeColumnRelation, groupAttributes, ref sqlInstruction);
                else
                    sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

                if (!string.IsNullOrEmpty(orderAttributes))
                    ParseOrdinationAttributes(attributeColumnRelation, orderAttributes, orderDescending, ref sqlInstruction);
                else
                    sqlInstruction = string.Format(sqlInstruction, string.Empty);

                return sqlInstruction;
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region Helper Methods

        private static string GetSqlInstruction(object entity, Type entityType, PropertyInfo[] entityProps, PersistenceAction action, object filterEntity, string[] showAttributes, IDictionary<string, double[]> rangeValues, string groupAttributes, bool readUncommited = false)
        {
            string sqlInstruction;
            Dictionary<object, object> sqlFilterData;
            Dictionary<object, object> sqlEntityData = EntityReflector.GetPropertiesValueList(entity, entityType, entityProps, action);

            if (filterEntity != null)
                sqlFilterData = EntityReflector.GetPropertiesValueList(filterEntity, entityType, entityProps, action);
            else
                sqlFilterData = null;

            var keyColumnName = EntityReflector.GetKeyColumnName(entityProps);

            Dictionary<string, string> sqlParameters = GetSqlParameters(sqlEntityData, action, sqlFilterData,
                                                                        showAttributes, keyColumnName,
                                                                        rangeValues, groupAttributes, readUncommited);
            switch (action)
            {
                case PersistenceAction.Create:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Create,
                                                   sqlParameters["TableName"],
                                                   sqlParameters["ColumnList"],
                                                   sqlParameters["ValueList"]);

                    break;

                case PersistenceAction.Edit:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Edit,
                                                   sqlParameters["TableName"],
                                                   sqlParameters["ColumnValueList"],
                                                   sqlParameters["ColumnFilterList"]);

                    break;

                case PersistenceAction.Delete:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Delete,
                                                   sqlParameters["TableName"],
                                                   sqlParameters["ColumnFilterList"]);

                    break;
                default: // Listagem ou Consulta

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Query,
                                                   sqlParameters["ColumnList"],
                                                   sqlParameters["TableName"],
                                                   sqlParameters["RelationList"],
                                                   sqlParameters["ColumnFilterList"],
                                                   "{0}", "{1}", string.Empty);

                    break;
            }

            return sqlInstruction;
        }
        
        private static void ParseGroupingAttributes(Dictionary<object, object> attributeColumnRelation, string groupAttributes, ref string sqlInstruction)
        {
            string columnList = string.Empty;
            string complementaryColumnList = string.Empty;
            string[] groupingAttributes = groupAttributes.Split(',');

            for (int cont = 0; cont < groupingAttributes.Length; cont++)
                groupingAttributes[cont] = groupingAttributes[cont].Trim();

            foreach (var rel in attributeColumnRelation)
                if (Array.IndexOf(groupingAttributes, rel.Key) > -1)
                    columnList += string.Format("{0}, ", ((KeyValuePair<object, object>)rel.Value).Key);
                else
                    if (!rel.Key.Equals("TableName"))
                    complementaryColumnList += string.Format("{0}, ", ((KeyValuePair<object, object>)rel.Value).Key);

            if (!string.IsNullOrEmpty(columnList) && (columnList.Length > 2))
                columnList = columnList.Substring(0, columnList.Length - 2);
            if (!string.IsNullOrEmpty(complementaryColumnList) && (complementaryColumnList.Length > 2))
                complementaryColumnList = complementaryColumnList.Substring(0, complementaryColumnList.Length - 2);

            sqlInstruction = string.Format(sqlInstruction,
                                           string.Format(SQLStatements.SQL_Action_Group,
                                                         columnList, ", ", complementaryColumnList),
                                                         "{0}");
        }

        private static void ParseOrdinationAttributes(Dictionary<object, object> attributeColumnRelation, string orderAttributes, bool orderDescending, ref string sqlInstruction)
        {
            string columnList = string.Empty;
            string[] ordinationAttributes = orderAttributes.Split(',');

            for (int contAtrib = 0; contAtrib < ordinationAttributes.Length; contAtrib++)
            {
                ordinationAttributes[contAtrib] = ordinationAttributes[contAtrib].Trim();

                var attribToOrder = attributeColumnRelation.FirstOrDefault(rca => ordinationAttributes[contAtrib].Equals(rca.Key));
                var columnToOrder = ((KeyValuePair<object, object>)attribToOrder.Value).Key;

                if (!(columnToOrder is RelationalColumn))
                    columnList = string.Concat(columnList, columnToOrder, ", ");
                else
                    columnList = string.Concat(columnList, string.Format("{0}.{1}", ((RelationalColumn)columnToOrder).TableName.ToLower(),
                                                                                    ((RelationalColumn)columnToOrder).ColumnName), ", ");
            }

            columnList = columnList.Substring(0, columnList.Length - 2);

            sqlInstruction = string.Format(sqlInstruction,
                                           string.Format(SQLStatements.SQL_Action_OrderResult,
                                                         columnList,
                                                         orderDescending ? "DESC" : "ASC"));
        }

        private static Dictionary<string, string> GetSqlParameters(Dictionary<object, object> entitySqlData, PersistenceAction action, IDictionary<object, object> entitySqlFilter, string[] showAttributes, string keyColumnName, IDictionary<string, double[]> rangeValues, string groupAttributes, bool readUncommited = false)
        {
            var returnDictionary = new Dictionary<string, string>();

            string tableName = string.Empty;
            string columnList = string.Empty;
            string valueList = string.Empty;
            string columnValueList = string.Empty;
            string columnFilterList = string.Empty;
            string relationList = string.Empty;

            string entityColumnName = string.Empty;
            string entityAttributeName = string.Empty;

            if (entitySqlData != null)
                foreach (var item in entitySqlData)
                {
                    KeyValuePair<object, object> itemChildKeyPair;

                    // Grouping predicate
                    if (!item.Key.Equals("TableName"))
                    {
                        itemChildKeyPair = (KeyValuePair<object, object>)item.Value;

                        entityAttributeName = item.Key.ToString();
                        entityColumnName = ((KeyValuePair<object, object>)item.Value).Key.ToString();

                        if (!string.IsNullOrWhiteSpace(groupAttributes) && groupAttributes.Contains(entityAttributeName))
                            columnList += string.Format("{0}.{1}, ", tableName, entityColumnName);
                    }

                    if (item.Key.Equals("TableName"))
                    {
                        returnDictionary.Add(item.Key.ToString(), item.Value.ToString());
                        tableName = item.Value.ToString();
                    }
                    else if (itemChildKeyPair.Key is RelationalColumn)
                    {
                        GetRelationalSqlParameters(itemChildKeyPair, tableName, ref columnList, ref relationList);
                    }
                    else if (itemChildKeyPair.Key is DataAggregationColumn)
                    {
                        GetAggregationSqlParameters(itemChildKeyPair, tableName, entityAttributeName, ref columnList);
                    }
                    else
                    {
                        GetPredicateSqlParameters(itemChildKeyPair, action, tableName, keyColumnName, entityColumnName,
                                                  entityAttributeName, showAttributes, ref columnList, ref valueList, ref columnValueList);
                    }
                }

            if (entitySqlFilter != null)
                GetFilterSqlParameters(entitySqlFilter, tableName, action, rangeValues, ref columnFilterList);

            FillSqlParametersResult(returnDictionary, action, ref columnList, ref valueList, ref columnValueList, ref columnFilterList, ref relationList, readUncommited);

            return returnDictionary;
        }

        public static object ParseManyToRelation(object childEntity, RelatedEntity relation)
        {
            object result = null;
            var relEntity = relation.IntermediaryEntity;

            if (relEntity != null)
            {
                var interEntity = Activator.CreateInstance(relation.IntermediaryEntity);

                var childProps = childEntity.GetType().GetProperties();
                var childKey = EntityReflector.GetKeyColumn(childProps);
                var interKeyAttrib = interEntity.GetType().GetProperties()
                                                .FirstOrDefault(atb => atb.Name.Equals(relation.IntermediaryKeyAttribute));

                interKeyAttrib.SetValue(interEntity, childKey.GetValue(childEntity, null), null);

                result = interEntity;
            }

            return result;
        }

        public static PersistenceAction SetPersistenceAction(object entity, PropertyInfo entityKeyColumn)
        {
            return (entityKeyColumn.GetValue(entity, null).ToString().Equals(SqlDefaultValue.Zero))
                    ? PersistenceAction.Create : PersistenceAction.Edit;
        }

        private static void FillSqlParametersResult(IDictionary<string, string> returnDictionary, PersistenceAction action, ref string columnList, ref string valueList, ref string columnValueList, ref string columnFilterList, ref string relationList, bool readUncommited = false)
        {
            if (action == PersistenceAction.Create)
            {
                columnList = columnList.Substring(0, columnList.Length - 2);
                valueList = valueList.Substring(0, valueList.Length - 2);

                returnDictionary.Add("ColumnList", columnList);
                returnDictionary.Add("ValueList", valueList);
            }
            else
            {
                if ((action == PersistenceAction.List)
                    || (action == PersistenceAction.Get)
                    || (action == PersistenceAction.Count))
                {
                    columnList = columnList.Substring(0, columnList.Length - 2);
                    returnDictionary.Add("ColumnList", columnList);
                    returnDictionary.Add("RelationList", relationList);

                    if (readUncommited)
                        returnDictionary["TableName"] = string.Concat(returnDictionary["TableName"], " (NOLOCK)");
                }
                else if (!string.IsNullOrEmpty(columnValueList))
                {
                    columnValueList = columnValueList.Substring(0, columnValueList.Length - 2);
                    returnDictionary.Add("ColumnValueList", columnValueList);
                }

                if (!string.IsNullOrEmpty(columnFilterList))
                {
                    var tokenRemove = (action == PersistenceAction.List)
                                       ? SqlOperator.Or.Length
                                       : SqlOperator.And.Length;

                    columnFilterList = columnFilterList.Substring(0, columnFilterList.Length - tokenRemove);

                    returnDictionary.Add("ColumnFilterList", columnFilterList);
                }
                else
                    returnDictionary.Add("ColumnFilterList", "1 = 1");
            }
        }

        private static void GetPredicateSqlParameters(KeyValuePair<object, object> itemChildKeyPair, PersistenceAction action, string tableName, string keyColumnName, string entityColumnName, string entityAttributeName, string[] showAttributes, ref string columnList, ref string valueList, ref string columnValueList)
        {
            object entityColumnValue = itemChildKeyPair.Value;
            var isCustomColumn = !entityAttributeName.Equals(entityColumnName);

            if ((showAttributes != null) && (showAttributes.Length > 0))
                for (int counter = 0; counter < showAttributes.Length; counter++)
                    showAttributes[counter] = showAttributes[counter].Trim();

            switch (action)
            {
                case PersistenceAction.Create:
                    columnList += string.Format("{0}, ", entityColumnName);
                    valueList += string.Format("{0}, ", entityColumnValue);

                    break;
                case PersistenceAction.List:

                    if (((showAttributes == null) || (showAttributes.Length == 0))
                        || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                    {
                        var columnAlias = isCustomColumn ? string.Format(" AS {0}", entityAttributeName) : string.Empty;
                        columnList += string.Format("{0}.{1}{2}, ", tableName, entityColumnName, columnAlias);
                    }

                    break;
                case PersistenceAction.Get:

                    if (((showAttributes == null) || showAttributes.Length == 0)
                        || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                    {
                        var columnAlias = isCustomColumn ? string.Format(" AS {0}", entityAttributeName) : string.Empty;
                        columnList += string.Format("{0}.{1}{2}, ", tableName, entityColumnName, columnAlias);
                    }

                    break;
                case PersistenceAction.Count:

                    if (entityColumnName.Equals(keyColumnName))
                        columnList += string.Format(SQLStatements.SQL_Action_CountAggregation,
                                                    tableName, entityColumnName, entityAttributeName);

                    break;
                default: // Alteração e Exclusão
                    if (!entityAttributeName.ToLower().Equals("id"))
                    {
                        if (entityColumnValue == null)
                            entityColumnValue = SqlDefaultValue.Null;

                        columnValueList += string.Format("{0} = {1}, ", entityColumnName, entityColumnValue);
                    }

                    break;
            }
        }

        private static void GetFilterSqlParameters(IDictionary<object, object> entitySqlFilter, string tableName, PersistenceAction action, IDictionary<string, double[]> rangeValues, ref string columnFilterList)
        {
            foreach (var filter in entitySqlFilter)
            {
                if (!filter.Key.Equals("TableName") && !filter.Key.Equals("RelatedEntity"))
                {
                    object filterColumnName = null;
                    object filterColumnValue = null;
                    object columnName = null;
                    string columnNameStr = string.Empty;

                    var itemChildKeyPair = (KeyValuePair<object, object>)filter.Value;
                    if (!(itemChildKeyPair.Key is RelationalColumn))
                    {
                        columnName = itemChildKeyPair.Key;
                        filterColumnName = string.Concat(tableName, ".", columnName);
                        filterColumnValue = itemChildKeyPair.Value;
                    }
                    else
                    {
                        RelationalColumn relationConfig = itemChildKeyPair.Key as RelationalColumn;

                        if ((action == PersistenceAction.List) && relationConfig.Filterable)
                        {
                            filterColumnName = string.Concat(relationConfig.TableName.ToLower(), ".", relationConfig.ColumnName);
                            filterColumnValue = itemChildKeyPair.Value;
                        }
                    }

                    var rangeFilter = false;
                    if (rangeValues != null)
                    {
                        columnNameStr = columnName.ToString();
                        rangeFilter = rangeValues.ContainsKey(columnNameStr);
                    }

                    if (((filterColumnValue != null)
                            && (filterColumnValue.ToString() != SqlDefaultValue.Null)
                            && (filterColumnValue.ToString() != SqlDefaultValue.Zero))
                        || rangeFilter)
                    {
                        long fake;
                        bool compareRule = ((action == PersistenceAction.List)
                                            || (action == PersistenceAction.Count))
                                         && !long.TryParse(filterColumnValue.ToString(), out fake)
                                         && !filterColumnName.ToString().ToLower().Contains("date")
                                         && !filterColumnName.ToString().ToLower().StartsWith("id")
                                         && !filterColumnName.ToString().ToLower().EndsWith("id")
                                         && !filterColumnName.ToString().ToLower().Contains(".id");

                        string comparation = string.Empty;

                        if (!rangeFilter)
                        {
                            comparation = (compareRule)
                                          ? string.Format(SqlOperator.Contains, filterColumnValue.ToString().Replace("'", string.Empty))
                                          : string.Concat(SqlOperator.Equal, filterColumnValue);

                            if (filterColumnValue.Equals(true))
                                comparation = " = 1";

                            if ((action == PersistenceAction.Edit) && filterColumnValue.Equals(false))
                                comparation = " = 0";

                            if (!filterColumnValue.Equals(false))
                                columnFilterList += filterColumnName + comparation +
                                    ((compareRule) ? SqlOperator.Or : SqlOperator.And);
                        }
                        else
                        {
                            double rangeFrom = rangeValues[columnNameStr][0];
                            double rangeTo = rangeValues[columnNameStr][1];

                            comparation = string.Format(SqlOperator.Between, rangeFrom, rangeTo);

                            columnFilterList += string.Concat(filterColumnName, " ", comparation, SqlOperator.And);
                        }
                    }
                }
            }
        }

        private static void GetRelationalSqlParameters(KeyValuePair<object, object> itemChildKeyPair, string tableName, ref string columnList, ref string relationList)
        {
            string relation;
            RelationalColumn relationConfig = itemChildKeyPair.Key as RelationalColumn;

            columnList += string.Format("{0}.{1} ", relationConfig.TableName.ToLower(), relationConfig.ColumnName);

            if (!string.IsNullOrEmpty(relationConfig.ColumnAlias))
                columnList += string.Format(SQLStatements.SQL_Action_ColumnAlias, relationConfig.ColumnAlias);

            columnList += ", ";

            if (relationConfig.JunctionType == RelationalJunctionType.Mandatory)
            {
                relation = string.Format(SQLStatements.SQL_Action_RelationateMandatorily,
                                                       relationConfig.TableName.ToLower(),
                                                       string.Concat(tableName, ".", relationConfig.KeyColumn),
                                                       string.Concat(relationConfig.TableName, ".",
                                                       relationConfig.ForeignKeyColumn, " "));
            }
            else
            {
                if (!string.IsNullOrEmpty(relationConfig.IntermediaryColumnName))
                {
                    relation = string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                             relationConfig.IntermediaryColumnName.ToLower(),
                                             string.Concat(tableName, ".", relationConfig.ForeignKeyColumn),
                                             string.Concat(relationConfig.IntermediaryColumnName, ".",
                                             relationConfig.ForeignKeyColumn));

                    relation += string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                              relationConfig.TableName,
                                              string.Concat(relationConfig.IntermediaryColumnName, ".", relationConfig.KeyColumn),
                                              string.Concat(relationConfig.TableName, ".", relationConfig.ForeignKeyColumn, " "));
                }
                else
                {
                    relation = string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                             relationConfig.TableName,
                                             string.Concat(tableName, ".", relationConfig.KeyColumn),
                                             string.Concat(relationConfig.TableName, ".", relationConfig.ForeignKeyColumn));
                }
            }

            if (relation.Contains(relationList)
                || string.IsNullOrEmpty(relationList))
                relationList = relation;
            else if (!relationList.Contains(relation))
                relationList += relation;
        }

        private static void GetAggregationSqlParameters(KeyValuePair<object, object> itemChildKeyPair, string tableName, string entityAttributeName, ref string columnList)
        {
            var annotation = itemChildKeyPair.Key as DataAggregationColumn;

            switch (annotation.AggregationType)
            {
                case DataAggregationType.Count:
                    columnList += string.Format(SQLStatements.SQL_Action_CountAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
                case DataAggregationType.Sum:
                    columnList += string.Format(SQLStatements.SQL_Action_SummaryAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
                case DataAggregationType.Average:
                    columnList += string.Format(SQLStatements.SQL_Action_AverageAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
                case DataAggregationType.Minimum:
                    columnList += string.Format(SQLStatements.SQL_Action_MinimumAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
                case DataAggregationType.Maximum:
                    columnList += string.Format(SQLStatements.SQL_Action_MaximumAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
            }
        }

        #endregion
    }
}
