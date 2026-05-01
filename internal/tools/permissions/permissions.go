package permissions

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"

	"github.com/BrianDeacon/databricks-utils-mcp-go/internal/client"
)

func GetGrants(ctx context.Context, securableType, fullName string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	resp, err := w.Grants.Get(ctx, catalog.GetGrantRequest{
		SecurableType: string(securableType),
		FullName:      fullName,
	})
	if err != nil {
		return fmt.Sprintf("Error getting grants: %v", err)
	}

	type grantInfo struct {
		Principal  string   `json:"principal"`
		Privileges []string `json:"privileges"`
	}
	var grants []grantInfo
	for _, g := range resp.PrivilegeAssignments {
		var privs []string
		for _, p := range g.Privileges {
			privs = append(privs, string(p))
		}
		grants = append(grants, grantInfo{
			Principal:  g.Principal,
			Privileges: privs,
		})
	}

	if len(grants) == 0 {
		return fmt.Sprintf("No grants found on %s '%s'.", securableType, fullName)
	}

	out, _ := json.MarshalIndent(grants, "", "  ")
	return string(out)
}

func GetObjectPermissions(ctx context.Context, objectType, objectID string, host, profile, tokenEnvVar *string) string {
	w, err := client.GetClient(host, profile, tokenEnvVar)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	resp, err := w.Permissions.Get(ctx, iam.GetPermissionRequest{
		RequestObjectType: objectType,
		RequestObjectId:   objectID,
	})
	if err != nil {
		return fmt.Sprintf("Error getting permissions: %v", err)
	}

	type permInfo struct {
		PermissionLevel string `json:"permission_level,omitempty"`
		Inherited       bool   `json:"inherited"`
	}
	type aclEntry struct {
		UserName              string     `json:"user_name,omitempty"`
		GroupName             string     `json:"group_name,omitempty"`
		ServicePrincipalName  string     `json:"service_principal_name,omitempty"`
		Permissions           []permInfo `json:"permissions"`
	}
	var acl []aclEntry
	for _, item := range resp.AccessControlList {
		var perms []permInfo
		for _, p := range item.AllPermissions {
			perms = append(perms, permInfo{
				PermissionLevel: string(p.PermissionLevel),
				Inherited:       p.Inherited,
			})
		}
		acl = append(acl, aclEntry{
			UserName:             item.UserName,
			GroupName:            item.GroupName,
			ServicePrincipalName: item.ServicePrincipalName,
			Permissions:          perms,
		})
	}

	if len(acl) == 0 {
		return fmt.Sprintf("No permissions found on %s '%s'.", objectType, objectID)
	}

	out, _ := json.MarshalIndent(acl, "", "  ")
	return string(out)
}
