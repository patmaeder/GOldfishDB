package processors

import (
	"DBMS/storage"
	"sort"
)

type OrderInstruction struct {
	Column    [128]byte
	Direction OrderDirection
}

type OrderDirection int

const (
	ASC OrderDirection = iota
	DESC
)

type OrderProcessor struct {
	Rows         []storage.Row
	Instructions []OrderInstruction
}

func Order(instructions []OrderInstruction) OrderProcessor {
	for i := 0; i < len(instructions)/2; i++ {
		instructions[i], instructions[len(instructions)-i-1] = instructions[len(instructions)-i-1], instructions[i]
	}

	return OrderProcessor{
		Instructions: instructions,
	}
}

func (p *OrderProcessor) Process(rows []storage.Row) ([]storage.Row, error) {
	for _, instruction := range p.Instructions {
		switch instruction.Direction {
		case ASC:
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i].Entries[instruction.Column].Smaller(rows[j].Entries[instruction.Column])
			})
		case DESC:
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i].Entries[instruction.Column].Greater(rows[j].Entries[instruction.Column])
			})
		}
	}

	return rows, nil
}
