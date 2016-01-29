package organisations

const (
	FullOrgUuid    = "4e484678-cf47-4168-b844-6adb47f8eb58"
	MinimalOrgUuid = "33f93f25-3301-417e-9b20-50b27d215617"
	OddCharOrgUuid = "161403e2-074f-3c82-9328-0337e909ac8c"
)

var FsIdentifier = identifier{
	Authority:       FsAuthority,
	IdentifierValue: "identifierValue",
}

var LeiCodeIdentifier = identifier{
	Authority:       LeiIdentifier,
	IdentifierValue: "LeiCodeIdentifier",
}

var FullOrg = organisation{
	UUID:                   FullOrgUuid,
	Type:                   TypePublicCompany,
	Identifiers:            []identifier{FsIdentifier, LeiCodeIdentifier},
	ProperName:             "Proper Name",
	LegalName:              "Legal Name",
	ShortName:              "Short Name",
	HiddenLabel:            "Hidden Label",
	FormerNames:            []string{"Old Name, inc.", "Older Name, inc."},
	TradeNames:             []string{"Old Trade Name, inc.", "Older Trade Name, inc."},
	LocalNames:             []string{"Oldé Name, inc.", "Tradé Name"},
	Aliases:                []string{"alias1", "alias2", "alias3"},
	ParentOrganisation:     "de38231e-e481-4958-b470-e124b2ef5a34",
	IndustryClassification: "c3d17865-f9d1-42f2-9ca2-4801cb5aacc0",
}

var MinimalOrg = organisation{
	UUID:        MinimalOrgUuid,
	Type:        TypeOrganisation,
	Identifiers: []identifier{FsIdentifier},
	ProperName:  "Proper Name",
}
