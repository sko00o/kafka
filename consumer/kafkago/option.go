package kafkago

type OptionFunc func(*Handler) error

func WithLogger(log Logger) OptionFunc {
	return func(h *Handler) error {
		h.log = log
		return nil
	}
}
