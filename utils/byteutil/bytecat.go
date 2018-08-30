package byteutil

//ByteCat join to []byte
func ByteCat(des []byte, src []byte) []byte {
	for _, c := range src {
		des = append(des, c)
	}
	return des
}
