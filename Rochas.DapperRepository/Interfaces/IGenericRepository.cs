using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rochas.DapperRepository.Interfaces
{
    public interface IGenericRepository<T> where T : class
    {
        int Create(T entity);
        Task<int> CreateAsync(T entity);
        int Delete(T filterEntity);
        Task<int> DeleteAsync(T filterEntity);
        int Edit(T entity, T filterEntity);
        Task<int> EditAsync(T entity, T filterEntity);
        T Get(T filter, bool loadComposition = false);
        Task<T> GetAsync(T filter, bool loadComposition = false);
        IEnumerable<T> List(T filter, bool loadComposition, int recordsLimit = 0, string orderAttributes = null, bool orderDescending = false);
        Task<IEnumerable<T>> ListAsync(T filter, bool loadComposition, int recordsLimit = 0, string orderAttributes = null, bool orderDescending = false);
    }
}