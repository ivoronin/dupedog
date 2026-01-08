package progress

import (
	"fmt"
	"os"
	"time"

	"github.com/schollz/progressbar/v3"
)

const updateInterval = 50 * time.Millisecond

// Bar wraps progressbar with enabled/disabled handling.
// All methods are no-ops when disabled.
type Bar struct {
	bar *progressbar.ProgressBar
}

// New creates a progress bar.
// If enabled=false, returns a Bar where all methods are no-ops.
// Use total=-1 for spinner mode, or total>0 for determinate progress.
func New(enabled bool, total int64) *Bar {
	if !enabled {
		return &Bar{}
	}

	opts := []progressbar.Option{
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionThrottle(updateInterval),
		progressbar.OptionClearOnFinish(),
	}

	if total < 0 {
		// Spinner mode
		opts = append(opts,
			progressbar.OptionSpinnerType(14),
			progressbar.OptionSetElapsedTime(false),
		)
		return &Bar{bar: progressbar.NewOptions(-1, opts...)}
	}

	// Progress bar mode
	opts = append(opts, progressbar.OptionSetWidth(40))
	return &Bar{bar: progressbar.NewOptions64(total, opts...)}
}

// Set sets the progress bar to a specific value.
func (b *Bar) Set(n uint64) {
	if b.bar != nil {
		_ = b.bar.Set64(int64(n))
	}
}

// Describe updates the progress bar description.
func (b *Bar) Describe(s fmt.Stringer) {
	if b.bar != nil {
		b.bar.Describe(s.String())
	}
}

// Finish completes the progress bar and prints a final message.
func (b *Bar) Finish(s fmt.Stringer) {
	if b.bar != nil {
		_ = b.bar.Finish()
		fmt.Fprintln(os.Stderr, "✔ "+s.String())
	}
}
