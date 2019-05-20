@search @usefixtures(workbook)
Feature: Report
    Background:
        When I visit "/report/?type=Snowball"
        And I wait for the content to load


    Scenario: Report
        Then the title should contain the text "Report"
        Then I should see at least 22 elements with the css selector "tbody > tr"


    Scenario: Report Snowballs
        When I click the link to "?type=Snowball&method=hand-packed"
        And I wait for the content to load
        Then I should see at least 8 elements with the css selector "tbody > tr"

        When I click the link to "?type=Snowball&method=hand-packed&method=scoop-formed"
        And I wait for the content to load
        Then I should see at least 12 elements with the css selector "tbody > tr"
