package relay

import "strings"

type MeasurementKey string

func NewMeasurementKey(name string, tvs ...string) MeasurementKey {
	sb := strings.Builder{}
	sb.WriteString(name)
	if len(tvs) < 2 {
		return MeasurementKey(sb.String())
	}
	sb.WriteByte(' ')
	for i := 0; i < len(tvs)/2; i++ {
		if i != 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(tvs[i*2])
		sb.WriteByte('=')
		sb.WriteString(tvs[i*2+1])
	}
	return MeasurementKey(sb.String())
}

func (m MeasurementKey) Measurement() string {
	s := string(m)
	idx := strings.IndexByte(s, ' ')
	if idx == -1 {
		return s
	}
	return s[0:idx]
}

func (m MeasurementKey) Tags() map[string]string {
	s := string(m)
	idx := strings.IndexByte(s, ' ')
	if idx == -1 {
		return nil
	}
	s = s[idx+1:]
	tags := make(map[string]string)
	for {
		idx = strings.IndexByte(s, '=')
		if idx == -1 {
			return tags
		}
		tk := s[0:idx]
		s = s[idx+1:]
		idx = strings.IndexByte(s, ',')
		if idx == -1 {
			if s != "" {
				tags[tk] = s
			}
			return tags
		}
		tv := s[0:idx]
		tags[tk] = tv
		s = s[idx+1:]
	}
}
