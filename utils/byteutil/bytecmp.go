package byteutil

//ByteCmp compare two bytes, the following is the return value:
//	0	b1 == b2
//  <0	b1 < b2
//	>0	b1 > b2
func ByteCmp(b1 []byte, b2 []byte) int {
	i := 0
	for i = 0; i < len(b1) && i < len(b2); i++ {
		if b1[i] < b2[i] {
			return -1
		} else if b1[i] > b2[i] {
			return 1
		}
	}
	if i == len(b1) && i == len(b2) {
		return 0
	} else if i == len(b1) {
		return -1
	} else {
		return 1
	}
}
