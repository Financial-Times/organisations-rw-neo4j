package roles

// Role is the representation of a Job Role
/*public class Role {
    public final String uuid;
    public final boolean isBoardRole;
    public final String prefLabel;
    public final String identifier;
}*/
type role struct {
	UUID                   string                 `json:"uuid"`
	IsBoardRole            bool                   `json:"isBoardRole"`
	PrefLabel              string                 `json:"prefLabel,omitempty"`
	AlternativeIdentifiers alternativeIdentifiers `json:"alternativeIdentifiers"`
	Types                  []string               `json:"types,omitempty"`
}

type alternativeIdentifiers struct {
	FactsetIdentifier string   `json:"factsetIdentifier,omitempty"`
	UUIDS             []string `json:"uuids"`
}

const (
	factsetIdentifierLabel = "FactsetIdentifier"
	uppIdentifierLabel     = "UPPIdentifier"
)
