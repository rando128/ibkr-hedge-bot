@smoke
Feature: Smoke test for the site

    As a developer,
    I want to see the site working,
    So that I know the application is not down.

    Scenario: Shows correct Django admin page
        Given I am logged in as a Django admin
        Then I should see the text "welcome"
        And I should not see the text "log in"
        And I should be at a URL with "back/admin/"
        And I should see the following Django admin models:
            | Group name                       | Model name         |
            | Authentication and Authorization | Groups             |
            | Procrastinate                    | Procrastinate jobs |

    Scenario: Non-admin user can't log in to Django admin
        Given I am the following user:
            | email    | good2@user.com |
            | is_admin | no             |
            | password | correct        |
        And I am on the /back/admin page
        When I log in with good2@user.com and correct
        Then I should see the text "Please enter the correct email address and password for a staff account"


