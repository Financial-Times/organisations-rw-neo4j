package organisations

import "errors"
import "sort"

type OrgType string

type SortedIdentifiers []identifier

type organisation struct {
	UUID                   string       `json:"uuid"`
	Type                   OrgType      `json:"type"`
	ProperName             string       `json:"properName"`
	LegalName              string       `json:"legalName,omitempty"`
	ShortName              string       `json:"shortName,omitempty"`
	HiddenLabel            string       `json:"hiddenLabel,omitempty"`
	Identifiers            []identifier `json:"identifiers"`
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

func sortIdentifiers(iden []identifier) {
	sort.Sort(SortedIdentifiers(iden))
}

// these three are the implementation of sort interface
func (si SortedIdentifiers) Len() int {
	return len(si)
}

func (si SortedIdentifiers) Swap(i, j int) {
	si[i], si[j] = si[j], si[i]
}

func (si SortedIdentifiers) Less(i, j int) bool {

	if si[i].Authority == si[j].Authority {
		return si[i].IdentifierValue < si[j].IdentifierValue
	} else {
		return si[i].Authority < si[j].Authority
	}
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
		return errors.New("This type is not supported yet. Only 'Organisation', 'Company' or 'PublicCompany' and these types must be allocated to a 'type' json property"), ""
	}
}

const (
	PublicCompany OrgType = "PublicCompany"
	Company       OrgType = "Company"
	Organisation  OrgType = "Organisation"
)
