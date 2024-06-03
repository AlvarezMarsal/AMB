using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmbHelper
{
    public class ColumnDefinitionCollection : IList<ColumnDefinition>
    {
        List<ColumnDefinition> _columnDefinitions = new List<ColumnDefinition>();

        public ColumnDefinition this[int index] 
        { 
            get => ((IList<ColumnDefinition>)_columnDefinitions)[index]; 
            set => ((IList<ColumnDefinition>)_columnDefinitions)[index] = value; 
        }

        public int Count => ((ICollection<ColumnDefinition>)_columnDefinitions).Count;

        public bool IsReadOnly => ((ICollection<ColumnDefinition>)_columnDefinitions).IsReadOnly;

        public void Add(ColumnDefinition item)
        {
            ((ICollection<ColumnDefinition>)_columnDefinitions).Add(item);
        }

        public void Clear()
        {
            ((ICollection<ColumnDefinition>)_columnDefinitions).Clear();
        }

        public bool Contains(ColumnDefinition item)
        {
            return ((ICollection<ColumnDefinition>)_columnDefinitions).Contains(item);
        }

        public void CopyTo(ColumnDefinition[] array, int arrayIndex)
        {
            ((ICollection<ColumnDefinition>)_columnDefinitions).CopyTo(array, arrayIndex);
        }

        public IEnumerator<ColumnDefinition> GetEnumerator()
        {
            return ((IEnumerable<ColumnDefinition>)_columnDefinitions).GetEnumerator();
        }

        public int IndexOf(ColumnDefinition item)
        {
            return ((IList<ColumnDefinition>)_columnDefinitions).IndexOf(item);
        }

        public void Insert(int index, ColumnDefinition item)
        {
            ((IList<ColumnDefinition>)_columnDefinitions).Insert(index, item);
        }

        public bool Remove(ColumnDefinition item)
        {
            return ((ICollection<ColumnDefinition>)_columnDefinitions).Remove(item);
        }

        public void RemoveAt(int index)
        {
            ((IList<ColumnDefinition>)_columnDefinitions).RemoveAt(index);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)_columnDefinitions).GetEnumerator();
        }

        public bool TryGetValue(string columnName, [MaybeNullWhen(false)] out ColumnDefinition columnDefinition)
        {
            for (var i=0; i<_columnDefinitions.Count; i++)
            {
                if (_columnDefinitions[i].ColumnName == columnName)
                {
                    columnDefinition = _columnDefinitions[i];
                    return true;
                }
            }

            for (var i=0; i<_columnDefinitions.Count; i++)
            {
                if (_columnDefinitions[i].Tag == columnName)
                {
                    columnDefinition = _columnDefinitions[i];
                    return true;
                }
            }

            columnDefinition = null;
            return false;
        }

        public bool TryAdd(ColumnDefinition columnDefinition)
        {
            if (!TryGetValue(columnDefinition.ColumnName, out _) &&
                !TryGetValue(columnDefinition.Tag, out _))
            {
                Add(columnDefinition);
                return true;
            }
            return false;
        }

        public ColumnDefinition this[string nameOrTag]
        {
            get
            {
                if (TryGetValue(nameOrTag, out var columnDefinition))
                    return columnDefinition;
                throw new KeyNotFoundException($"Column '{nameOrTag}' not found");
            }
        }
    }
}
