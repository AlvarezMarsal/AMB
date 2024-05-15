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
    Region, 
    City
}

internal record GeographicLocation
{
    public readonly GeographicLocation? Parent;
    public readonly long Oid;
    public readonly string Name;
    public readonly int Index;
    public readonly string Description;
    public readonly long PracticeAreaId;
    public readonly GeographicLocationKind Kind;
    public readonly SortedList<string, GeographicLocation> Children = [];
    public override string ToString() => $"{Name} ({Oid})";

    public GeographicLocation(GeographicLocation? parent, long oid, string name, int index, string description, long practiceAreaId)
    {
        Parent = parent;
        Kind = (parent == null) ? GeographicLocationKind.World : (parent.Kind+1);
        Oid = oid;
        Name = name;
        Index = index;
        Description = description;
        PracticeAreaId = practiceAreaId;
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

