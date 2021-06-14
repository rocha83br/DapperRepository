using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IGenericRepository<T> where T : class
    {
        void Initialize(string databaseFileName, string tableScript);
        int Count(T filterEntity);
        Task<int> CountAsync(T filterEntity);
        int Create(T entity, bool persistComposition = false);
        Task<int> CreateAsync(T entity, bool persistComposition = false);
        void CreateRange(IEnumerable<T> entities, bool persistComposition = false);
        Task CreateRangeAsync(IEnumerable<T> entities, bool persistComposition = false);
        int Delete(T filterEntity);
        Task<int> DeleteAsync(T filterEntity);
        int Edit(T entity, T filterEntity, bool persistComposition = false);
        Task<int> EditAsync(T entity, T filterEntity, bool persistComposition = false);
        T Get(object key, bool loadComposition = false);
        Task<T> GetAsync(object key, bool loadComposition = false);
        T Get(T filter, bool loadComposition = false);
        Task<T> GetAsync(T filter, bool loadComposition = false);
        ICollection<T> Search(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        Task<ICollection<T>> SearchAsync(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        ICollection<T> List(T filter, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        Task<ICollection<T>> ListAsync(T filter, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
    }
}