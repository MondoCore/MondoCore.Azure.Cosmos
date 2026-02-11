using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;
using MondoCore.Data;

namespace MondoCore.Azure.Cosmos
{
   internal class CosmosContainerReader<TID, TValue> : CosmosContainer<TID>, IReadRepository<TID, TValue> where TValue : IIdentifiable<TID>
    {
        internal CosmosContainerReader(Container container, IIdentifierStrategy<TID> strategy) : base(container, strategy)
        {
        }

        #region IReadRepository

        public Task<TValue> Get(TID id, CancellationToken cancellationToken = default)
        {
            var idResult = SplitId(id);

            return InternalGet<TValue>(idResult.Id, idResult.PartitionKey, cancellationToken);
        }

        public async IAsyncEnumerable<TValue> Get(IEnumerable<TID> ids, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach(var id in ids)
            { 
                if(cancellationToken.IsCancellationRequested)
                    yield break;

                yield return await Get(id);
            }
        }

        public IAsyncEnumerable<TValue> Get(Expression<Func<TValue, bool>> query, CancellationToken cancellationToken = default)
        {
            return InternalGet<TValue>(query, cancellationToken);
        }

        #region IQueryable<>

        #region IQueryable

        public Type             ElementType => typeof(TValue);
        public Expression       Expression  => this.Container.GetItemLinqQueryable<TValue>(true).Expression;
        public IQueryProvider   Provider    => this.Container.GetItemLinqQueryable<TValue>(true).Provider;

        #endregion

        #region IEnumerable<>

        public IEnumerator<TValue> GetEnumerator() => this.Container.GetItemLinqQueryable<TValue>(true).GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
          => this.Container.GetItemLinqQueryable<TValue>(true).GetEnumerator();

        #endregion

        #endregion

        #endregion
    }

}
