using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;
using MondoCore.Common;
using MondoCore.Data;

namespace MondoCore.Azure.Cosmos
{
   internal class CosmosContainerWriter<TID, TValue> : CosmosContainer<TID>, IWriteRepository<TID, TValue> where TValue : IIdentifiable<TID> 
    {
        internal CosmosContainerWriter(Container container, IIdentifierStrategy<TID> strategy) : base(container, strategy)
        {
        }

       #region IWriteRepository

        public async Task<bool> Delete(TID id)
        {
            var idResult = SplitId(id);
                
            await this.Container.DeleteItemAsync<TValue>(idResult.Id, idResult.PartitionKey);

            return true;
        }

        public async Task<long> Delete(Expression<Func<TValue, bool>> guard)
        {
            var result = InternalGet<TValue>(guard);
            var count = 0L;

            await Parallel.ForEachAsync(result, async (val, token)=>
            {
                var partitionKey = GetPartitionKey(val);

                try
                { 
                    await this.Container.DeleteItemAsync<TValue>(GetId(val), partitionKey);

                    Interlocked.Increment(ref count);
                }
                catch
                {
                }
            });

            return count;
        }

        public async Task<TValue> Insert(TValue item)
        {
            var result = await this.Container.CreateItemAsync(item);

            return result.Resource;
        }

        public async Task Insert(IEnumerable<TValue> items)
        {
            foreach(var item in items)
                await Insert(item);
        }

        public async Task<bool> Update(TValue item, Expression<Func<TValue, bool>> guard = null)
        {
            if(guard != null)
            { 
                var currentItem = await InternalGet<TValue>(GetId(item), GetPartitionKey(item));
                var list        = (new List<TValue> {currentItem}) as IEnumerable<TValue>;
                var fnGuard     = guard.Compile();

                if(!list.Where(fnGuard).Any())
                    return false;
            }

            var partitionKey = GetPartitionKey(item);

            try
            { 
                var result = await this.Container.UpsertItemAsync(item, partitionKey);

                return result.StatusCode == System.Net.HttpStatusCode.OK;
            }
            catch(CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException();
            }
        }

        public async Task<long> Update(object properties, Expression<Func<TValue, bool>> query)
        {
            var result = InternalGet<TValue>(query); 
            var count = 0L;

            await Parallel.ForEachAsync(result, async (val, token)=>
            {
                try
                { 
                    if(val.SetValues(properties))
                    { 
                        await this.Container.UpsertItemAsync<TValue>(val);
                        Interlocked.Increment(ref count);
                    }
                }
                catch
                {
                }
            });

            return count;
        }

        public async Task<long> Update(Func<TValue, Task<(bool Update, bool Continue)>> update, Expression<Func<TValue, bool>> query)
        {
            var result = InternalGet<TValue>(query); 
            var count = 0L;
            
            await Parallel.ForEachAsync(result, async (val, token)=>
            {
                try
                { 
                    var result = await update(val);

                    if(result.Update)
                    { 
                        await this.Container.UpsertItemAsync<TValue>(val);
                        Interlocked.Increment(ref count);
                    }
                }
                catch
                {
                }
            });

            return count;
        }

        #endregion

        private PartitionKey GetPartitionKey(TValue item)
        {
            if(item is IPartitionable<TID> partitionable)
                return new PartitionKey(partitionable.GetPartitionKey());

            return PartitionKey.None;
        }
    }
}
