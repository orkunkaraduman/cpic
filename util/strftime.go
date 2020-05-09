package util

import (
	"fmt"
	"strings"
	"time"
)

var strftimeConversion = map[rune]string{
	'a': "Mon",
	'A': "Monday",
	'b': "Jan",
	'B': "January",
	'c': "Mon Jan _2 15:04:05 2006",
	//'C': "",
	'd': "02",
	'D': "01/02/06",
	'e': "_2",
	//'E': "",
	//'f': "", // none
	'F': "2006-01-02",
	'h': "Jan",
	'H': "15",
	//'i': "", // none
	'I': "03",
	//'j': "",
	//'J': "", // none
	'k': "15", // special
	//'K': "", // none
	'l': "3",
	//'L': "", // none
	'm': "01",
	'M': "04",
	'n': "\n",
	//'N': "", // none
	//'o': "", // none
	//'O': "",
	'p': "PM",
	'P': "pm",
	'r': "03:04:05 PM",
	'R': "15:04",
	//'s': "",
	'S': "05",
	't': "\t",
	'T': "15:04:05",
	//'u': "",
	//'U': "",
	//'v': "", // none
	//'V': "",
	//'w': "",
	//'W': "",
	'x': "01/02/06",
	'X': "15:04:05",
	'y': "06",
	'Y': "2006",
	'z': "-0700",
	'Z': "MST",

	'f': ".000000", // special
	'L': ".000",    // special
}

func Strftime(format string, t time.Time) string {
	retval := make([]byte, 0, len(format))
	for i, ni := 0, 0; i < len(format); i = ni + 2 {
		ni = strings.IndexByte(format[i:], '%')
		if ni < 0 {
			ni = len(format)
		} else {
			ni += i
		}
		retval = append(retval, []byte(format[i:ni])...)
		if ni+1 < len(format) {
			c := format[ni+1]
			if c == '%' {
				retval = append(retval, '%')
			} else {
				if layoutCmd, ok := strftimeConversion[rune(c)]; ok {
					var s string
					switch rune(c) {
					case 'f':
						s = fmt.Sprintf("%06d", t.Nanosecond()/1000)
					case 'k':
						s = fmt.Sprintf("%d", t.Hour())
					case 'L':
						s = fmt.Sprintf("%03d", t.Nanosecond()/1000000)
					default:
						s = t.Format(layoutCmd)
					}
					retval = append(retval, []byte(s)...)
				} else {
					retval = append(retval, '%', c)
				}
			}
		} else {
			if ni < len(format) {
				retval = append(retval, '%')
			}
		}
	}
	return string(retval)
}
