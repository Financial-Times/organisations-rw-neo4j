package roles

// Role is the representation of a Job Role
/*public class Role {
    public final String uuid;
    public final boolean isBoardRole;
    public final String prefLabel;
    public final String identifier;
}*/
type role struct {
	UUID        string       `json:"uuid"`
	IsBoardRole bool         `json:"isBoardRole"`
	Identifiers []identifier `json:"identifiers,omitempty"`
	PrefLabel   string       `json:"prefLabel,omitempty"`
}

type identifier struct {
	Authority       string `json:"authority"`
	IdentifierValue string `json:"identifierValue"`
}
