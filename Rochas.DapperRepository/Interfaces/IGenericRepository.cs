using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rochas.DapperRepository.Interfaces
{
    public interface IGenericRepository<T> where T : class
    {
        int Count(T filterEntity);
        Task<int> CountAsync(T filterEntity);
        int Create(T entity);
        Task<int> CreateAsync(T entity);
        void CreateRange(IEnumerable<T> entities);
        Task CreateRangeAsync(IEnumerable<T> entities);
        int Delete(T filterEntity);
        Task<int> DeleteAsync(T filterEntity);
        int Edit(T entity, T filterEntity);
        Task<int> EditAsync(T entity, T filterEntity);
        T Get(T filter, bool loadComposition = false);
        Task<T> GetAsync(T filter, bool loadComposition = false);
        ICollection<T> List(T filter, bool loadComposition, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        Task<ICollection<T>> ListAsync(T filter, bool loadComposition, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
    }
}