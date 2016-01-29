package organisations

import "errors"

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

func (o OrgType) String() (error, string) {

	switch o {
	case Organisation:
		return nil, "Organisation:Concept:Thing"
	case Company:
		return nil, "Company:Organisation:Concept:Thing"
	case PublicCompany:
		return nil, "PublicCompany:Company:Organisation:Concept:Thing"
	default:
		return errors.New("Dissalowed Type"), ""
	}
}

const (
	PublicCompany OrgType = "PublicCompany"
	Company       OrgType = "Company"
	Organisation  OrgType = "Organisation"
)
