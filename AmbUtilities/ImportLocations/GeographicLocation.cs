using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace ImportLocations;

internal enum GeographicLocationKind
{
    World,
    Continent,
    Country,
    State,          // or privince or postal area or whatever
    County,         // or whatever is smaller than a State, if anything 
    City
}

internal class GeographicLocation
{
    public readonly long Oid;
    public readonly long? Pid;
    public readonly string Name;
    public readonly int Index;
    public readonly string Description;
    public readonly long PracticeAreaId;
    public readonly bool IsSystemOwned;
    //public readonly GeographicLocationKind Kind;
    public readonly SortedList<string, GeographicLocation> Children = [];
    public override string ToString() => $"{Name} ({Oid})";
    public bool ChildrenLoaded { get; set; } = false;

    public GeographicLocation(long oid, long? pid, string name, int index, string description, long practiceAreaId, bool isSystemOwned)
    {
        Oid = oid;
        Pid = pid;
        //Kind = (parent == null) ? GeographicLocationKind.World : (parent.Kind+1);
        Oid = oid;
        Name = name;
        Index = index;
        Description = description;
        PracticeAreaId = practiceAreaId;
        IsSystemOwned = isSystemOwned;
    }

    public void Walk(Action<GeographicLocation> action)
    {
        action(this);
        foreach (var child in Children)
        {
            child.Value.Walk(action);
        }
    }
}

