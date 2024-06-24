package strutils

func Truncate(inStr string, length int, withoutEllipses bool) (out string) {
	if len(inStr) > length {
		out = inStr[:length]
		if !withoutEllipses {
			out += "..."
		}
	} else {
		out = inStr[:] // safety!
	}
	return
}
