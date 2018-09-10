package gpdconst

import "github.com/erician/gpdDB/common/gpdtype"

//gpdDB operations, and the system operations is invisible to user
const (
	GET        int8 = 0 //GET PUT DELETE are all user operations
	PUT        int8 = 1
	DELETE     int8 = 2
	CHECKPOINT int8 = 3 //CHECKPOINT ALLOCATE and SETFIELD are all system operations
	ALLOCATE   int8 = 4
	SETFIELD   int8 = 5
)

//OperationEnum all operations in gpdDB including system operations
var OperationEnum = []gpdtype.Enum{
	{Name: "GET", Value: GET},
	{Name: "PUT", Value: PUT},
	{Name: "DELETE", Value: DELETE},
	{Name: "CHECKPOINT", Value: CHECKPOINT},
	{Name: "ALLOCATE", Value: ALLOCATE},
}
