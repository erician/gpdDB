package readwrite

//ReadByte is to read readLen bytes from src with starting pos: srcStart
func ReadByte(src []byte, srcStart int, readLen int) (des []byte, err error) {
	des = make([]byte, readLen)
	err = WriteByte(des, 0, src, srcStart, readLen)
	return
}
