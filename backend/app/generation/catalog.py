from app.schemas.lesson import ActivityType, VisualizationType


REGISTERED_VISUALIZATIONS = {
    VisualizationType.STEP_BY_STEP,
    VisualizationType.ANNOTATED_DIAGRAM,
    VisualizationType.COMPARISON_TABLE,
    VisualizationType.DECISION_MATRIX,
    VisualizationType.TIMELINE,
    VisualizationType.CODE_WALKTHROUGH,
    VisualizationType.CALLOUT,
    VisualizationType.REQUEST_FLOW,
    VisualizationType.SERVER_CLUSTER,
    VisualizationType.LOAD_DISTRIBUTION,
}

SPECIALIZED_VISUALIZATIONS = {
    VisualizationType.REQUEST_FLOW,
    VisualizationType.SERVER_CLUSTER,
    VisualizationType.LOAD_DISTRIBUTION,
}

REGISTERED_ACTIVITIES = {
    ActivityType.SIMULATION,
    ActivityType.QUIZ,
    ActivityType.STEPPER,
}

