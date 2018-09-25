package gpdconst

import "github.com/erician/gpdDB/common/gpdtype"

//gpdDB transactions
const (
	USERTRANS int8 = 0 //GET PUT DELETE are all user operations
	OPTRANS   int8 = 1
	SYSTRANS  int8 = 2
)

//OperationEnum all operations in gpdDB including system operations
var TransEnum = []gpdtype.Enum{
	{Name: "USERTRANS", Value: USERTRANS},
	{Name: "OPTRANS", Value: OPTRANS},
	{Name: "SYSTRANS", Value: SYSTRANS},
}
