package migration

import "fmt"

type ErrCreateTable struct {
	Err       error
	TableName string
}

func (err ErrCreateTable) Unwrap() error { return err.Err }
func (err ErrCreateTable) Error() string {
	return fmt.Sprintf("failed to created table %v : %v", err.TableName, err.Err)
}

type ErrTrackingInfo struct {
	Err       error
	TableName string
}

func (err ErrTrackingInfo) Unwrap() error { return err.Err }
func (err ErrTrackingInfo) Error() string {
	return fmt.Sprintf("failed inserting tracking info %v : %v", err.TableName, err.Err)
}

type ErrApplyFileRead struct {
	Filename string
	Err      error
}

func (err ErrApplyFileRead) Unwrap() error { return err.Err }
func (err ErrApplyFileRead) Error() string {
	return fmt.Sprintf("failed to read file %v: %v", err.Filename, err.Err)
}

type ErrApplyFileTemplate struct {
	Filename string
	Err      error
}

func (err ErrApplyFileTemplate) Unwrap() error { return err.Err }
func (err ErrApplyFileTemplate) Error() string {
	return fmt.Sprintf("failed to parse file %v: %v", err.Filename, err.Err)
}

type ErrApplyFile struct {
	Filename string
	Sha1Hash string
	Err      error
}

func (err ErrApplyFile) Unwrap() error { return err.Err }
func (err ErrApplyFile) Error() string {
	return fmt.Sprintf("failed to apply file %v [%v]: %v", err.Filename, err.Sha1Hash, err.Err)
}

type ErrUnknownVersion string

func (err ErrUnknownVersion) Error() string {
	return fmt.Sprintf("unknown db version: `%v`", string(err))
}
