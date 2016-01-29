package organisations

type OrgType string

type organisation struct {
	UUID                   string       `json:"uuid"`
	Type                   OrgType      `json:"type"`
	ProperName             string       `json:"properName"`
	LegalName              string       `json:"legalName,omitempty"`
	ShortName              string       `json:"shortName,omitempty"`
	HiddenLabel            string       `json:"hiddenLabel,omitempty"`
	Identifiers            []identifier `json:"identifiers,omitempty"`
	TradeNames             []string     `json:"tradeNames,omitempty"`
	LocalNames             []string     `json:"localNames,omitempty"`
	FormerNames            []string     `json:"formerNames,omitempty"`
	Aliases                []string     `json:"aliases,omitempty"`
	IndustryClassification string       `json:"industryClassification,omitempty"`
	ParentOrganisation     string       `json:"parentOrganisation,omitempty"`
}

type identifier struct {
	Authority       string `json:"authority"`
	IdentifierValue string `json:"identifierValue"`
}

func (o OrgType) String() string {

	switch o {
	case TypeOrganisation:
		return "Organisation:Concept:Thing"
	case TypeCompany:
		return "Company:Organisation:Concept:Thing"
	case TypePublicCompany:
		return "PublicCompany:Company:Organisation:Concept:Thing"
	default:
		return "Thing"
	}
}

const (
	FsAuthority               = "http://api.ft.com/system/FACTSET-EDM"
	LeiIdentifier             = "http://api.ft.com/system/LEI"
	TypePublicCompany OrgType = "PublicCompany"
	TypeCompany       OrgType = "Company"
	TypeOrganisation  OrgType = "Organisation"
)
