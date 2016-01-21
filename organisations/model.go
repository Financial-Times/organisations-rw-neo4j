package organisations

type OrgType string

type organisation struct {
	UUID                   string       `json:"uuid"`
	Type                   OrgType      `json:"type"`
	PrefLabel              string       `json:"properName"`
	ProperName             string       `json:"properName"`
	LegalName              string       `json:"legalName"`
	ShortName              string       `json:"shortName"`
	HiddenLabel            string       `json:"hiddenLabel"`
	Identifiers            []identifier `json:"identifiers,omitempty"`
	TradeNames             []string     `json:"tradeNames"`
	LocalNames             []string     `json:"localNames"`
	FormerNames            []string     `json:"formerNames"`
	TmeLabels              []string     `json:"tmeLabels"`
	IndustryClassification string       `json:"industryClassification"`
	ParentOrganisation     string       `json:"parentOrganisation"`
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
