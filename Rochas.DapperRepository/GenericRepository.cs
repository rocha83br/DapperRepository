using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;
using System.IO;
using Newtonsoft.Json;
using Rochas.DapperRepository.Base;
using Rochas.DapperRepository.Helpers;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Interfaces;
using Rochas.DapperRepository.Specification.Annotations;
using Rochas.DapperRepository.Helpers.SQL;
using System.Data.SQLite;

namespace Rochas.DapperRepository
{
    public class GenericRepository<T> : DataBaseConnection, IDisposable, IGenericRepository<T> where T : class
    {
        #region Declarations

        static Type entityType = typeof(T);
        static PropertyInfo[] entityProps = entityType.GetProperties();
        bool _readUncommited;

        #endregion

        #region Constructors

        public GenericRepository(DatabaseEngine engine, string connectionString, string logPath = null, bool keepConnected = false, bool readUncommited = false, params string[] replicaConnStrings)
            : base(engine, connectionString, logPath, keepConnected, replicaConnStrings)
        {
            _readUncommited = readUncommited;
        }

        #endregion

        #region Public Methods

        public void Initialize(string tableScript, string databaseFileName = null)
        {
            if (!string.IsNullOrWhiteSpace(databaseFileName))
            {
                if (File.Exists(databaseFileName))
                    File.Delete(databaseFileName);

                SQLiteConnection.CreateFile(databaseFileName);
            }

            Connect();
            ExecuteCommand(tableScript);
            Disconnect();
        }

        public T Get(object key, bool loadComposition = false)
        {
            var filter = EntityReflector.GetFilterByPrimaryKey(entityType, entityProps, key) as T;

            return Get(filter, loadComposition);
        }

        public async Task<T> GetAsync(object key, bool loadComposition = false)
        {
            var filter = EntityReflector.GetFilterByPrimaryKey(entityType, entityProps, key) as T;

            return await GetAsync(filter, loadComposition);
        }

        public T Get(T filter, bool loadComposition = false)
        {
            return GetObject(filter, loadComposition) as T;
        }

        public async Task<T> GetAsync(T filter, bool loadComposition = false)
        {
            return await GetObjectAsync(filter, loadComposition) as T;
        }
        public ICollection<T> Search(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false)
        {
            var result = new List<T>();
            var filter = EntityReflector.GetFilterByFilterableColumns(entityType, entityProps, criteria);
            var queryResult = ListObjects(filter, PersistenceAction.List, loadComposition, recordsLimit, sortAttributes: sortAttributes, orderDescending: orderDescending);
            if (queryResult != null)
                foreach (var item in queryResult)
                    result.Add(item as T);

            return result;
        }
        public async Task<ICollection<T>> SearchAsync(object criteria, bool loadComposition = false, int recordsLimit = 0, string orderAttributes = null, bool orderDescending = false)
        {
            var result = new List<T>();
            var filter = EntityReflector.GetFilterByFilterableColumns(entityType, entityProps, criteria);
            var queryResult = await ListObjectsAsync(filter, PersistenceAction.List, loadComposition, recordsLimit, orderAttributes: orderAttributes, orderDescending: orderDescending);
            if (queryResult != null)
                foreach (var item in queryResult)
                    result.Add(item as T);

            return result;
        }

        public ICollection<T> List(T filter, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false)
        {
            var result = new List<T>();
            var queryResult = ListObjects(filter, PersistenceAction.List, loadComposition, recordsLimit, sortAttributes: sortAttributes, orderDescending: orderDescending);
            if (queryResult != null)
                foreach (var item in queryResult)
                    result.Add(item as T);

            return result;
        }
        public async Task<ICollection<T>> ListAsync(T filter, bool loadComposition = false, int recordsLimit = 0, string orderAttributes = null, bool orderDescending = false)
        {
            var result = new List<T>();
            var queryResult = await ListObjectsAsync(filter, PersistenceAction.List, loadComposition, recordsLimit, orderAttributes: orderAttributes, orderDescending: orderDescending);
            if (queryResult != null)
                foreach (var item in queryResult)
                    result.Add(item as T);

            return result;
        }

        public int Create(T entity, bool persistComposition = false)
        {
            return CreateObject(entity, persistComposition);
        }

        public async Task<int> CreateAsync(T entity, bool persistComposition = false)
        {
            return await CreateObjectAsync(entity, persistComposition);
        }

        public void CreateRange(IEnumerable<T> entities, bool persistComposition = false)
        {
            try
            {
                StartTransaction();

                foreach (var entity in entities)
                    CreateObject(entity, persistComposition);

                CommitTransaction();
            }
            catch (Exception ex)
            {
                CancelTransaction();
                throw ex;
            }
        }

        public void CreateBulkRange(ICollection<T> entities)
        {
            var entitiesTable = EntityReflector.GetDataTable<T>(entities);
            ExecuteBulkCommand(entitiesTable);
        }

        public async Task CreateBulkRangeAsync(ICollection<T> entities)
        {
            var entitiesTable = EntityReflector.GetDataTable<T>(entities);
            await ExecuteBulkCommandAsync(entitiesTable);
        }

        public async Task CreateRangeAsync(IEnumerable<T> entities, bool persistComposition = false)
        {
            try
            {
                StartTransaction();

                foreach (var entity in entities)
                    await CreateObjectAsync(entity, persistComposition);

                CommitTransaction();
            }
            catch (Exception ex)
            {
                CancelTransaction();
                throw ex;
            }
        }

        public int Edit(T entity, T filterEntity, bool persistComposition = false)
        {
            return EditObject(entity, filterEntity, persistComposition);
        }

        public async Task<int> EditAsync(T entity, T filterEntity, bool persistComposition = false)
        {
            return await EditObjectAsync(entity, filterEntity, persistComposition);
        }

        public int Delete(T filterEntity)
        {
            return Delete(filterEntity as object);
        }

        public async Task<int> DeleteAsync(T filterEntity)
        {
            return await DeleteAsync(filterEntity as object);
        }

        public int Count(T filterEntity)
        {
            return Count(filterEntity as object);
        }

        public async Task<int> CountAsync(T filterEntity)
        {
            return await CountAsync(filterEntity as object);
        }

        #endregion

        #region Helper Methods

        private object GetObject(object filter, bool loadComposition = false)
        {
            return ListObjects(filter, PersistenceAction.Get, loadComposition)?.FirstOrDefault();
        }

        private async Task<object> GetObjectAsync(object filter, bool loadComposition = false)
        {
            var queryResult = await ListObjectsAsync(filter, PersistenceAction.Get, loadComposition);
            return queryResult?.FirstOrDefault();
        }

        private IEnumerable<object> ListObjects(object filterEntity, PersistenceAction action, bool loadComposition = false, int recordLimit = 0, bool onlyListableAttributes = false, string showAttributes = null, Dictionary<string, double[]> rangeValues = null, string groupAttributes = null, string sortAttributes = null, bool orderDescending = false)
        {
            IEnumerable<object> returnList = null;

            // Getting SQL statement from Helper
            var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, action, filterEntity, recordLimit, onlyListableAttributes, showAttributes, rangeValues, groupAttributes, sortAttributes, orderDescending, _readUncommited);

            if (keepConnection || Connect())
            {
                // Getting database return using Dapper
                returnList = ExecuteQuery(filterEntity.GetType(), sqlInstruction);
            }

            if (!keepConnection) Disconnect();

            // Perform the composition data load when exists (Eager Loading)
            if (loadComposition && (returnList != null) && returnList.Any())
            {
                var itemProps = returnList.First().GetType().GetProperties();
                foreach (var item in returnList)
                    FillComposition(item, itemProps);
            }

            return returnList;
        }

        private async Task<IEnumerable<object>> ListObjectsAsync(object filterEntity, PersistenceAction action, bool loadComposition = false, int recordLimit = 0, bool onlyListableAttributes = false, string showAttributes = null, Dictionary<string, double[]> rangeValues = null, string groupAttributes = null, string orderAttributes = null, bool orderDescending = false)
        {
            IEnumerable<object> returnList = null;

            var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, action, filterEntity, recordLimit, onlyListableAttributes, showAttributes, rangeValues, groupAttributes, orderAttributes, orderDescending, _readUncommited);

            if (keepConnection || base.Connect())
            {
                // Getting database return using Dapper
                returnList = await ExecuteQueryAsync(filterEntity.GetType(), sqlInstruction);
            }

            if (!keepConnection) base.Disconnect();

            // Perform the composition data load when exists (Eager Loading)
            if (loadComposition && (returnList != null) && returnList.Any())
            {
                var itemProps = returnList.First().GetType().GetProperties();
                foreach (var item in returnList)
                    FillComposition(item, itemProps);
            }

            return returnList;
        }

        private int CreateObject(object entity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
        {
            string sqlInstruction;
            int lastInsertedId = 0;

            var entityType = entity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Create);

                if (persistComposition)
                    base.StartTransaction();

                lastInsertedId = ExecuteCommand(sqlInstruction);

                if (persistComposition)
                    PersistComposition(entity, PersistenceAction.Create);
                else
                    if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(entity);

            // Async persistence of database replicas
            if (replicationEnabled && !isReplicating)
                CreateReplicas(entity, entityProps, lastInsertedId, persistComposition);

            return lastInsertedId;
        }

        private async Task<int> CreateObjectAsync(object entity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
        {
            string sqlInstruction;
            int lastInsertedId = 0;

            var entityType = entity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Create);

                if (persistComposition)
                    base.StartTransaction();

                lastInsertedId = await ExecuteCommandAsync(sqlInstruction);

                if (persistComposition)
                    PersistComposition(entity, PersistenceAction.Create);
                else
                    if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(entity);

            // Async persistence of database replicas
            if (replicationEnabled && !isReplicating)
                CreateReplicas(entity, entityProps, lastInsertedId, persistComposition);

            return lastInsertedId;
        }

        private int EditObject(object entity, object filterEntity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
        {
            int recordsAffected = 0;
            string sqlInstruction;

            var entityType = entity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Edit, filterEntity);

                if (persistComposition)
                    base.StartTransaction();

                recordsAffected = ExecuteCommand(sqlInstruction);

                if (persistComposition)
                    PersistComposition(entity, PersistenceAction.Edit);
                else
                if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(entity);

            // Async persistence of database replicas
            if (base.replicationEnabled && !isReplicating)
                EditReplicas(entity, filterEntity, entityProps, persistComposition);

            return recordsAffected;
        }

        private async Task<int> EditObjectAsync(object entity, object filterEntity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
        {
            int recordsAffected = 0;
            string sqlInstruction;

            var entityType = entity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Edit, filterEntity);

                if (persistComposition)
                    base.StartTransaction();

                recordsAffected = await ExecuteCommandAsync(sqlInstruction);

                if (persistComposition)
                    PersistComposition(entity, PersistenceAction.Edit);
                else
                if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(entity);

            // Async persistence of database replicas
            if (base.replicationEnabled && !isReplicating)
                EditReplicas(entity, filterEntity, entityProps, persistComposition);

            return recordsAffected;
        }

        private int Delete(object filterEntity, string optionalConnConfig = "", bool isReplicating = false)
        {
            string sqlInstruction;
            int recordsAffected = 0;

            var entityType = filterEntity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Delete, filterEntity);

                recordsAffected = ExecuteCommand(sqlInstruction);

                PersistComposition(filterEntity, PersistenceAction.Delete);

                if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(filterEntity);

            // Async persistence of database replicas
            if (base.replicationEnabled && !isReplicating)
                DeleteReplicas(filterEntity, entityProps);

            return recordsAffected;
        }

        private async Task<int> DeleteAsync(object filterEntity, string optionalConnConfig = "", bool isReplicating = false)
        {
            string sqlInstruction;
            int recordsAffected = 0;

            var entityType = filterEntity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Delete, filterEntity);

                recordsAffected = await ExecuteCommandAsync(sqlInstruction);

                PersistComposition(filterEntity, PersistenceAction.Delete);

                if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(filterEntity);

            // Async persistence of database replicas
            if (base.replicationEnabled && !isReplicating)
                DeleteReplicas(filterEntity, entityProps);

            return recordsAffected;
        }

        public int Count(object filterEntity)
        {
            int result = 0;

            // Getting SQL statement from Helper
            var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Count, filterEntity);

            if (keepConnection || Connect())
            {
                // Getting database return using Dapper
                result = ExecuteCommand(sqlInstruction);
            }

            if (!keepConnection) Disconnect();

            return result;
        }

        public async Task<int> CountAsync(object filterEntity)
        {
            int result = 0;

            // Getting SQL statement from Helper
            var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Count, filterEntity);

            if (keepConnection || Connect())
            {
                // Getting database return using Dapper
                result = await ExecuteCommandAsync(sqlInstruction);
            }

            if (!keepConnection) Disconnect();

            return result;
        }

        private void FillComposition(object loadedEntity, PropertyInfo[] entityProps)
        {
            RelatedEntity relationConfig = null;

            var propertiesList = entityProps.Where(prp => prp.GetCustomAttributes(true)
                                            .Any(atb => atb.GetType().Name.Equals("RelatedEntity")));

            foreach (var prop in propertiesList)
            {
                object attributeInstance = null;

                IEnumerable<object> attributeAnnotations = prop.GetCustomAttributes(true)
                                                               .Where(atb => atb.GetType().Name.Equals("RelatedEntity"));

                foreach (object annotation in attributeAnnotations)
                {
                    relationConfig = (RelatedEntity)annotation;

                    PropertyInfo foreignKeyColumn = null;
                    object foreignKeyValue = null;

                    var keyColumn = EntityReflector.GetKeyColumn(entityProps);

                    switch (relationConfig.Cardinality)
                    {
                        case RelationCardinality.OneToOne:

                            attributeInstance = Activator.CreateInstance(prop.PropertyType);

                            foreignKeyColumn = loadedEntity.GetType().GetProperty(relationConfig.ForeignKeyAttribute);

                            foreignKeyValue = foreignKeyColumn.GetValue(loadedEntity, null);

                            if ((foreignKeyValue != null) && int.Parse(foreignKeyValue.ToString()) > 0)
                            {
                                var attributeProps = attributeInstance.GetType().GetProperties();
                                var keyColumnAttribute = EntityReflector.GetKeyColumn(attributeProps);

                                keyColumnAttribute.SetValue(attributeInstance, foreignKeyColumn.GetValue(loadedEntity, null), null);

                                attributeInstance = GetObject(attributeInstance);
                            }

                            break;
                        case RelationCardinality.OneToMany:

                            attributeInstance = Activator.CreateInstance(prop.PropertyType.GetGenericArguments()[0], true);

                            foreignKeyColumn = attributeInstance.GetType().GetProperty(relationConfig.ForeignKeyAttribute);
                            foreignKeyColumn.SetValue(attributeInstance, int.Parse(keyColumn.GetValue(loadedEntity, null).ToString()), null);

                            attributeInstance = ListObjects(attributeInstance as object, PersistenceAction.List);

                            break;
                        case RelationCardinality.ManyToMany:

                            attributeInstance = Activator.CreateInstance(relationConfig.IntermediaryEntity, true);

                            if (attributeInstance != null)
                            {
                                SetEntityForeignKey(loadedEntity, attributeInstance);

                                var manyToRelations = ListObjects(attributeInstance, PersistenceAction.List, true);

                                Type childManyType = prop.PropertyType.GetGenericArguments()[0];
                                Type dynamicManyType = typeof(List<>).MakeGenericType(new Type[] { childManyType });
                                IList childManyEntities = (IList)Activator.CreateInstance(dynamicManyType, true);

                                foreach (var rel in manyToRelations)
                                {
                                    var childManyKeyValue = rel.GetType().GetProperty(relationConfig.IntermediaryKeyAttribute).GetValue(rel, null);
                                    var childFilter = Activator.CreateInstance(childManyType);

                                    var childFilterProps = childFilter.GetType().GetProperties();
                                    EntityReflector.GetKeyColumn(childFilterProps).SetValue(childFilter, childManyKeyValue, null);

                                    var childInstance = GetObject(childFilter);

                                    childManyEntities.Add(childInstance);
                                }

                                attributeInstance = childManyEntities;
                            }
                            break;
                    }
                }

                if (attributeInstance != null)
                    if (!prop.PropertyType.Name.Contains("List"))
                        prop.SetValue(loadedEntity, attributeInstance, null);
                    else
                        prop.SetValue(loadedEntity, (IList)attributeInstance, null);
            }
        }

        private List<string> ParseComposition(object entity, PersistenceAction action, object filterEntity)
        {
            List<string> result = new List<string>();
            object childEntityInstance = null;

            var entityType = entity.GetType();
            IEnumerable<PropertyInfo> childEntities = entityType.GetProperties().Where(prp => prp.GetCustomAttributes(true)
                                                                                .Any(atb => atb.GetType().Name.Equals("RelatedEntity")));

            foreach (PropertyInfo child in childEntities)
            {
                var relationAttrib = child.GetCustomAttributes(true)
                                          .FirstOrDefault(atb => atb.GetType().Name.Equals("RelatedEntity")) as RelatedEntity;

                childEntityInstance = child.GetValue(entity, null);
                object childEntityFilter = null;

                var entityParent = (action != PersistenceAction.Edit) ? entity : filterEntity;

                if (childEntityInstance != null)
                {
                    var childEntityType = childEntityInstance.GetType();

                    if (!childEntityType.Name.Contains("List"))
                    {
                        var childProps = childEntityType.GetProperties();
                        action = EntitySqlParser.SetPersistenceAction(childEntityInstance, EntityReflector.GetKeyColumn(childProps));
                        childEntityFilter = Activator.CreateInstance(childEntityInstance.GetType());

                        if (action == PersistenceAction.Edit)
                            EntityReflector.MigrateEntityPrimaryKey(childEntityInstance, childProps, childEntityFilter);

                        SetEntityForeignKey(entityParent, child);

                        result.Add(EntitySqlParser.ParseEntity(childEntityInstance, engine, action));
                    }
                    else
                    {
                        var childListInstance = (IList)childEntityInstance;
                        List<object> childFiltersList = new List<object>();

                        if (childListInstance.Count > 0)
                        {
                            foreach (var listItem in childListInstance)
                            {
                                if (relationAttrib.Cardinality == RelationCardinality.OneToMany)
                                {
                                    var listItemType = listItem.GetType();
                                    childEntityFilter = Activator.CreateInstance(listItemType);

                                    var listItemProps = listItemType.GetProperties();
                                    action = EntitySqlParser.SetPersistenceAction(listItem, EntityReflector.GetKeyColumn(listItemProps));

                                    if (action == PersistenceAction.Edit)
                                    {
                                        EntityReflector.MigrateEntityPrimaryKey(listItem, listItemProps, childEntityFilter);
                                        childFiltersList.Add(childEntityFilter);
                                    }

                                    SetEntityForeignKey(entityParent, listItem);

                                    result.Add(EntitySqlParser.ParseEntity(listItem, engine, action));
                                }
                                else
                                {
                                    var manyToEntity = EntitySqlParser.ParseManyToRelation(listItem, relationAttrib);

                                    SetEntityForeignKey(entityParent, manyToEntity);

                                    var existRelation = this.GetObject(manyToEntity);

                                    if (existRelation != null) manyToEntity = existRelation;

                                    var manyToEntityProps = manyToEntity.GetType().GetProperties();
                                    action = EntitySqlParser.SetPersistenceAction(manyToEntity, EntityReflector.GetKeyColumn(manyToEntityProps));

                                    object existFilter = null;
                                    if (action == PersistenceAction.Edit)
                                    {
                                        existFilter = Activator.CreateInstance(manyToEntity.GetType());
                                        EntityReflector.MigrateEntityPrimaryKey(manyToEntity, manyToEntityProps, existFilter);
                                        childFiltersList.Add(existFilter);
                                    }

                                    result.Add(EntitySqlParser.ParseEntity(manyToEntity, engine, action));
                                }
                            }
                        }
                        else
                        {
                            var childInstance = Activator.CreateInstance(childListInstance.GetType().GetGenericArguments()[0]);

                            var childEntity = new object();
                            if (relationAttrib.Cardinality == RelationCardinality.ManyToMany)
                                childEntity = EntitySqlParser.ParseManyToRelation(childInstance, relationAttrib);
                            else
                                childEntity = childInstance;

                            SetEntityForeignKey(entityParent, childEntity);

                            childFiltersList.Add(childEntity);
                        }
                    }
                }
            }

            if (result.Any(rst => rst.Contains(SQLStatements.SQL_ReservedWord_INSERT)))
                result.Reverse();

            return result;
        }

        private void CreateReplicas(object entity, PropertyInfo[] entityProps, int lastInsertedId, bool persistComposition)
        {
            var entityColumnKey = EntityReflector.GetKeyColumn(entityProps);
            if (entityColumnKey != null)
                entityColumnKey.SetValue(entity, lastInsertedId, null);

            ParallelParam parallelParam = new ParallelParam()
            {
                Param1 = entity,
                Param2 = PersistenceAction.Create,
                Param3 = persistComposition
            };

            var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

            Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);
        }

        private void EditReplicas(object entity, object filterEntity, PropertyInfo[] entityProps, bool persistComposition)
        {
            ParallelParam parallelParam = new ParallelParam()
            {
                Param1 = entity,
                Param2 = PersistenceAction.Edit,
                Param3 = persistComposition,
                Param4 = filterEntity
            };

            var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

            Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);

        }

        private void DeleteReplicas(object filterEntity, PropertyInfo[] entityProps)
        {
            ParallelParam parallelParam = new ParallelParam()
            {
                Param1 = filterEntity,
                Param2 = PersistenceAction.Delete
            };

            var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

            Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);
        }

        private void SetEntityForeignKey(object parentEntity, object childEntity)
        {
            var parentProps = parentEntity.GetType().GetProperties();
            var parentKey = EntityReflector.GetKeyColumn(parentProps);

            var childProps = childEntity.GetType().GetProperties();
            var childForeignKey = EntityReflector.GetForeignKeyColumn(childProps);

            if ((parentKey != null) && (childForeignKey != null))
                childForeignKey.SetValue(childEntity, parentKey.GetValue(parentEntity, null), null);
        }

        private void PersistComposition(object entity, PersistenceAction action, object filterEntity = null)
        {
            try
            {
                List<string> childEntityCommands = ParseComposition(entity, action, filterEntity);

                foreach (var cmd in childEntityCommands)
                    ExecuteCommand(cmd);

                if (base.transactionControl != null)
                    base.CommitTransaction();

                if (!keepConnection) base.Disconnect();

                // Efetua a limpeza do cache para a entidade em questão
                var isCacheable = (entity.GetType().GetCustomAttribute(typeof(CacheableAttribute)) != null);
                if (isCacheable)
                    DataCache.Del(entity, true);

            }
            catch (Exception)
            {
                if (base.transactionControl != null)
                    base.CancelTransaction();
            }
        }

        private void PersistReplicasAsync(object param)
        {
            try
            {
                foreach (var connString in _replicaConnStrings)
                {
                    ParallelParam parallelParam = param as ParallelParam;

                    object entity = parallelParam.Param1;
                    PersistenceAction action = (PersistenceAction)parallelParam.Param2;

                    bool persistComposition = false;
                    if (parallelParam.Param3 != null)
                        persistComposition = (bool)parallelParam.Param3;

                    object filterEntity = null;
                    if (parallelParam.Param4 != null)
                        filterEntity = parallelParam.Param4;

                    using (var repos = new GenericRepository<T>(DatabaseEngine.SQLServer, _connString))
                    {
                        switch (action)
                        {
                            case PersistenceAction.Create:
                                repos.CreateObject(entity, persistComposition, connString, true);
                                break;
                            case PersistenceAction.Edit:
                                repos.EditObject(entity, filterEntity, persistComposition, connString, true);
                                break;
                            case PersistenceAction.Delete:
                                repos.Delete(entity, connString, true);
                                break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                RegisterException("PersistReplicas", ex, param);
            }
        }

        private void CleanCacheableData(object entity)
        {
            var isCacheable = (entity.GetType().GetCustomAttribute(typeof(CacheableAttribute)) != null);
            if (isCacheable)
                DataCache.Del(entity, true);
        }

        private void RegisterException(string operationName, Exception exception, object content)
        {
            var logFileName = string.Format("{0}\\{1}_{2}_{3}.log", _logPath, operationName, content.GetHashCode(), DateTime.Now.Ticks);

            var exceptionContent = string.Format("Exception : {0}{1}{2} Content : {3}", JsonConvert.SerializeObject(exception), Environment.NewLine, Environment.NewLine, JsonConvert.SerializeObject(content));

            File.WriteAllText(logFileName, exceptionContent);
        }

        #endregion
    }
}
