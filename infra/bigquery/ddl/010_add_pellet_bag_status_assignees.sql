-- Add dedicated assignee columns for long moisture and density status workflows.
ALTER TABLE `PROJECT_ID.DATASET_ID.pellet_bags`
ADD COLUMN IF NOT EXISTS long_moisture_assignee_email STRING;

-- Add dedicated assignee columns for long moisture and density status workflows.
ALTER TABLE `PROJECT_ID.DATASET_ID.pellet_bags`
ADD COLUMN IF NOT EXISTS density_assignee_email STRING;
