package operator

import "data-replication/internal/enum"

const (
	Equal            enum.OperatorSQL = "="
	LessThan         enum.OperatorSQL = "<"
	GreaterThan      enum.OperatorSQL = ">"
	LessThanEqual    enum.OperatorSQL = "<="
	GreaterThanEqual enum.OperatorSQL = ">="
	Like             enum.OperatorSQL = "like"
)
