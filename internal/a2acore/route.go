package a2acore

// Route plans the deliveries of an admitted message from the explicit
// registrations: the project owner when one is registered, otherwise an
// owner no_route entry plus the guardian (or its no_route entry). A
// delivery to the caller's own reference is planned self (loop guard).
// A RouteSource error is returned as is, with no plan: the caller
// refuses and nothing is admitted.
func Route(src RouteSource, project, subjectRef string) ([]PlannedDelivery, error) {
	ownerRef, err := src.OwnerRef(project)
	if err != nil {
		return nil, err
	}
	// selfState marks a delivery to the caller's own reference so the
	// runner never sends a message back to its source.
	selfState := func(ref string) string {
		if subjectRef != "" && ref == subjectRef {
			return DeliverySelf
		}
		return DeliveryHeld
	}
	if ownerRef != "" {
		return []PlannedDelivery{{Role: RoleOwner, Ref: ownerRef, State: selfState(ownerRef)}}, nil
	}
	guardianRef, err := src.GuardianRef()
	if err != nil {
		return nil, err
	}
	plan := []PlannedDelivery{{Role: RoleOwner, Ref: "", State: DeliveryNoRoute}}
	if guardianRef != "" {
		plan = append(plan, PlannedDelivery{Role: RoleGuardian, Ref: guardianRef, State: selfState(guardianRef)})
	} else {
		plan = append(plan, PlannedDelivery{Role: RoleGuardian, Ref: "", State: DeliveryNoRoute})
	}
	return plan, nil
}
