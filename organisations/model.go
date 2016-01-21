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
	TmeLabels              []string     `json:"tmeLabels,omitempty"`
	IndustryClassification string       `json:"industryClassification,omitempty"`
	ParentOrganisation     string       `json:"parentOrganisation,omitempty"`
}

type identifier struct {
	Authority       string `json:"authority"`
	IdentifierValue string `json:"identifierValue"`
}

func (o OrgType) String() string {
	if o == Organisation {
		return "Organisation:Concept:Thing "
	} else if o == Company {
		return "Company:Organisation:Concept:Thing "
	} else if o == PublicCompany {
		return "PublicCompany:Company:Organisation:Concept:Thing "
	}
	return "Thing"
}

const (
	PublicCompany OrgType = "PublicCompany"
	Company       OrgType = "Company"
	Organisation  OrgType = "Organisation"
)
