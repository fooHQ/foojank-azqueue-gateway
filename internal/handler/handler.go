package handler

import (
	"context"
	"strings"

	protoagent "github.com/foohq/foojank-proto/go/agent"
	protogw "github.com/foohq/foojank-proto/go/gateway"

	"github.com/foohq/foojank-azqueue-gateway/internal/message"
)

type Fn func(context.Context, map[string]string, message.Msg) (message.Msg, error)

type Handler struct {
	routes map[string]Fn
}

func New() *Handler {
	h := &Handler{}
	h.routes = map[string]Fn{
		protoagent.CmdStartWorkerSubject("<gateway>", "<agent>", "<worker>"): h.HandleCmdStartWorker,
		protoagent.CmdStopWorkerSubject("<gateway>", "<agent>", "<worker>"):  h.HandleCmdStopWorker,
		protoagent.CmdWriteStdinSubject("<gateway>", "<agent>", "<worker>"):  h.HandleCmdWriteStdin,
		protogw.CmdRegisterAgentSubject("<gateway>", "<agent>"):              h.HandleCmdRegisterAgent,
		protogw.CmdUnregisterAgentSubject("<gateway>", "<agent>"):            h.HandleCmdUnregisterAgent,
	}
	return h
}

func (h *Handler) HandleCmdStartWorker(ctx context.Context, params map[string]string, msg message.Msg) (message.Msg, error) {
	return nil, nil
}

func (h *Handler) HandleCmdStopWorker(ctx context.Context, params map[string]string, msg message.Msg) (message.Msg, error) {
	return nil, nil
}
func (h *Handler) HandleCmdWriteStdin(ctx context.Context, params map[string]string, msg message.Msg) (message.Msg, error) {
	return nil, nil
}
func (h *Handler) HandleCmdRegisterAgent(ctx context.Context, params map[string]string, msg message.Msg) (message.Msg, error) {
	return nil, nil
}
func (h *Handler) HandleCmdUnregisterAgent(ctx context.Context, params map[string]string, msg message.Msg) (message.Msg, error) {
	return nil, nil
}

func (h *Handler) Match(msg message.Msg) (func(context.Context) (message.Msg, error), bool) {
	for route, handler := range h.routes {
		params, ok := match(route, msg.Subject())
		if ok {
			return func(ctx context.Context) (message.Msg, error) {
				return handler(ctx, params, msg)
			}, true
		}
	}
	return nil, false
}

func match(route, subject string) (map[string]string, bool) {
	// Split route and subject into segments
	routeParts := strings.Split(route, ".")
	subjectParts := strings.Split(subject, ".")

	// Check if lengths match
	if len(routeParts) != len(subjectParts) {
		return nil, false
	}

	// Initialize result map for variables
	params := make(map[string]string)

	// Compare each segment
	for i, routePart := range routeParts {
		// Check if routePart is a variable (starts with < and ends with >)
		if strings.HasPrefix(routePart, "<") && strings.HasSuffix(routePart, ">") {
			// Extract variable name (remove < and >)
			varName := strings.TrimPrefix(strings.TrimSuffix(routePart, ">"), "<")
			// Validate variable name doesn't contain < or >
			if strings.Contains(varName, "<") || strings.Contains(varName, ">") {
				return nil, false
			}
			// Store variable name and corresponding subject value
			params[varName] = subjectParts[i]
		} else {
			// If not a variable, segments must match exactly
			if routePart != subjectParts[i] {
				return nil, false
			}
		}
	}

	return params, true
}
