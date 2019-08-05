package ruleset

import (
	"container/list"

	"github.com/sirupsen/logrus"

	"github.com/carbonblack/eqr/logging"
	"github.com/carbonblack/eqr/metrics"
	plg "github.com/carbonblack/eqr/ruleset/pluginInterfaces"
	rl "github.com/carbonblack/eqr/ruleset/rulebase"
)

// Implementation of the Builder pattern for the Rules
type Rule struct {
	rule rl.Rulebase
}

var logger = logging.GetLogger()

// the Predicate and Projections all have steps that need to be performed
// so they will be stored in a list
func (r *Rule) InitRule() *Rule {
	r.rule.Predicate = list.New()
	r.rule.Projection = list.New()
	r.rule.Cache = list.New()
	return r
}

func (r *Rule) AddEmptyPredicate() *Rule {
	pb := rl.Base{Line: list.New()}
	r.rule.Predicate.PushBack(pb)

	return r
}

// Adds a Predicate; yes there can be multiple predicates
// that each have their own set of 'steps'
func (r *Rule) AddPredicate(step *rl.Step) *Rule {

	pb := rl.Base{Line: list.New()}
	pb.Line.PushBack(step)
	r.rule.Predicate.PushBack(pb)

	logger.WithFields(logrus.Fields{
	    "rule": (*r.rule.Destination.Plugin).Name(),
	}).Debug("Successfully added predicate")

	return r
}

// Adds a step to a Predicate
func (r *Rule) AddStep(step *rl.Step, isMulti bool) *Rule {
	t := r.rule.Predicate.Back()

	logger.WithFields(logrus.Fields{
	    "t": t,
		"isMulti": isMulti,
	}).Debug("The predicate back")

	if t == nil {
		return r.AddPredicate(step)
	}
	pb := t.Value.(rl.Base)

	if isMulti == false {
		pb.Line.PushBack(step)
	} else {
		proj := pb.Line.Back()
		if proj == nil {
			pb.Line.PushBack(step)
			proj = pb.Line.Back()
		}
		if (*proj.Value.(*rl.Step)).MultiArgs == nil {
			t := proj.Value.(*rl.Step)
			(*t).MultiArgs = list.New()
		}
		(*proj.Value.(*rl.Step)).MultiArgs.PushBack(step)
	}

	logger.WithFields(logrus.Fields{
	    "rule": (*r.rule.Destination.Plugin).Name(),
	}).Debug("Successfully added step")

	return r
}

// Adds a Projection step
func (r *Rule) AddProjectStep(step *rl.Step, isMulti bool) *Rule {
	t := r.rule.Projection.Back()
	pb := t.Value.(rl.Base)

	if isMulti == false {
		pb.Line.PushBack(step)
	} else {
		proj := pb.Line.Back()
		if proj == nil {
			pb.Line.PushBack(step)
			proj = pb.Line.Back()
		}
		if (*proj.Value.(*rl.Step)).MultiArgs == nil {
			t := proj.Value.(*rl.Step)
			(*t).MultiArgs = list.New()
		}
		(*proj.Value.(*rl.Step)).MultiArgs.PushBack(step)
	}

	logger.WithFields(logrus.Fields{
	    "rule": (*r.rule.Destination.Plugin).Name(),
	}).Debug("Successfully added projection step")

	return r
}

func (r *Rule) AddCacheStep(step *rl.Step, isMulti bool) *Rule {
	t := r.rule.Cache.Back()
	pb := t.Value.(rl.Base)

	if isMulti == false {
		pb.Line.PushBack(step)
	} else {
		proj := pb.Line.Back()
		if proj == nil {
			pb.Line.PushBack(step)
			proj = pb.Line.Back()
		}
		if (*proj.Value.(*rl.Step)).MultiArgs == nil {
			t := proj.Value.(*rl.Step)
			(*t).MultiArgs = list.New()
		}
		(*proj.Value.(*rl.Step)).MultiArgs.PushBack(step)
	}

	logger.WithFields(logrus.Fields{
	    "rule": (*r.rule.Destination.Plugin).Name(),
	}).Debug("Successfully added cache step")

	return r
}

// adds a step to another to complete an 'OR' statement
func (r *Rule) OrPredicate(step *rl.Step, isMulti bool) *Rule {
	firstStep := rl.Base{}
	firstStep.Line = list.New()
	firstStep.Line.PushBack(step)
	r.rule.Predicate.PushBack(firstStep)

	t := r.rule.Predicate.Back()
	pb := t.Value.(rl.Base)

	if isMulti == false {
		pb.Line.PushBack(step)
	} else {
		proj := pb.Line.Back()
		if proj == nil {
			pb.Line.PushBack(step)
			proj = pb.Line.Back()
		}
		if (*proj.Value.(*rl.Step)).MultiArgs == nil {
			t := proj.Value.(*rl.Step)
			(*t).MultiArgs = list.New()
		}
		(*proj.Value.(*rl.Step)).MultiArgs.PushBack(step)
	}

	logger.WithFields(logrus.Fields{
	    "rule": (*r.rule.Destination.Plugin).Name(),
	}).Debug("Successfully added OR predicate")

	return r
}

func (r *Rule) AddCache(cacheFunc *plg.PluginInterface, opName string) *Rule {
	var emptyString string
	pb := rl.Base{Line: list.New()}
	step := &rl.Step{
		Plugin:    cacheFunc,
		Value:     &emptyString,
		ID:        &opName,
		GetPrev:   false,
		MultiArgs: list.New(),
	}

	var tmp interface{}
	tmp = step
	pb.Line.PushBack(tmp)
	r.rule.Cache.PushBack(pb)

	logger.WithFields(logrus.Fields{
	    "rule": (*r.rule.Destination.Plugin).Name(),
	}).Debug("Successfully added cache")

	return r
}

// Adds a Projection
func (r *Rule) AddProjection() *Rule {
	pb := rl.Base{Line: list.New()}
	r.rule.Projection.PushBack(pb)

	logger.WithFields(logrus.Fields{
	    "rule": (*r.rule.Destination.Plugin).Name(),
	}).Debug("Successfully added projection")

	return r
}

// This is making the destination explicit and
// easy to retrieve
func (r *Rule) SetDestination(destPlugin *plg.IOPluginInterface, worker interface{}) *Rule {
	r.rule.Destination = &rl.Dest {
		Plugin: destPlugin,
		Pointer: worker,
	}

	return r
}

// This returns the rule
func (r *Rule) GetRule(name string, metricSender *metrics.SfxClient) rl.Rulebase {
	r.rule.MetricSender = metricSender
	r.rule.RuleName = name
	return r.rule
}