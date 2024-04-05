package org.neo4j.cypher.internal.administration

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.AdministrationCommandRuntime.makeCreateUserExecutionPlan
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler

// A case class designed for creating user execution plans within the Neo4j database.
// This class is a part of the administration tools that allow for detailed user management.
// It encapsulates the necessary components to generate execution plans for creating users.

case class DozerDbCreateUserExecutionPlanner(
  executionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler,
  config: Config
) {

  // This function generates an execution plan for creating a new user in the Neo4j database.
  // It takes in a CreateUser logical plan and an optional source plan, returning a finalized ExecutionPlan.
  // The creation includes setting up initial passwords, requiring password changes, and handling suspended states.
  // We use the same function name that Neo4j uses in core to make it easier to compare to.
  def planCreateUser(createUser: CreateUser, sourceExecutionPlan: Option[ExecutionPlan]): ExecutionPlan = {

    makeCreateUserExecutionPlan(
      createUser.userName,
      createUser.isEncryptedPassword,
      createUser.initialPassword,
      createUser.requirePasswordChange,
      suspended = createUser.suspended.getOrElse(false),
      createUser.defaultDatabase
    )(sourceExecutionPlan, executionEngine, securityAuthorizationHandler, config)

  }

}
