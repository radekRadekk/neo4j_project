import json

from neo4j import GraphDatabase
import csv

driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "password"))
set_file = "datasets/vb_matches_small.csv"


def delete_all(tx):
    tx.run("MATCH(n) DETACH DELETE n")


def add_circuit(tx, name):
    tx.run("CREATE (a:Circuit {name: $name}) ",
           name=name)


def add_country(tx, name):
    tx.run("CREATE (a:Country {name: $name}) ",
           name=name)


def add_tournament(tx, name, year):
    tx.run("CREATE (a:Tournament {name: $name, year: $year}) ",
           name=name, year=year)


def create_relation_tournament_country(tx, name, year, country):
    tx.run(
        "MATCH (t:Tournament), (c:Country) WHERE c.name = $country AND t.year = $year AND t.name = $name CREATE (t)-[r:IN_COUNTRY]->(c)",
        name=name, year=year, country=country)


def create_relation_tournament_circuit(tx, name, year, circuit):
    tx.run(
        "MATCH (t:Tournament), (c:Circuit) WHERE c.name = $circuit AND t.year = $year AND t.name = $name CREATE (t)-[r:IN_CIRCUIT]->(c)",
        name=name, year=year, circuit=circuit)


def add_player(tx, name, gender, birthdate, height_in):
    if birthdate == "NA":
        local_birthdate = ""
    else:
        local_birthdate = birthdate

    if height_in == "NA":
        local_height_in = ""
    else:
        local_height_in = height_in

    tx.run("CREATE (a:Player {name: $name, gender: $gender, birthdate: $birthdate, height_in: $height_in}) ",
           name=name, gender=gender, birthdate=local_birthdate, height_in=local_height_in)


def create_relation_player_country(tx, name, gender, birthdate, country):
    tx.run(
        "MATCH (p:Player), (c:Country) WHERE p.name = $name AND p.gender = $gender AND p.birthdate = $birthdate AND c.name = $country CREATE (p)-[r:FROM]->(c)",
        name=name, gender=gender, birthdate=birthdate, country=country)


def add_team(tx, name):
    tx.run("CREATE (a:Team {name: $name})", name=name)


def create_relation_team_member(tx, team_name, player1, player1_birthdate, player2, player2_birthdate):
    tx.run(
        "MATCH (t:Team), (p1:Player) WHERE t.name = $team_name AND p1.name = $player1 AND p1.birthdate = $player1_birthdate " +
        "CREATE (p1)-[r:MEMBER_IN]->(t)", team_name=team_name, player1=player1, player1_birthdate=player1_birthdate)
    tx.run("MATCH (t:Team), (p2:Player) WHERE t.name = $team_name " +
           "AND p2.name = $player2 AND p2.birthdate = $player2_birthdate CREATE (p2)-[r:MEMBER_IN]->(t)",
           team_name=team_name, player2=player2, player2_birthdate=player2_birthdate)


def add_match(tx, score, duration, date):
    result = tx.run("CREATE (m:Match {score: $score, duration: $duration, date: $date}) RETURN ID(m) AS id",
                    score=score,
                    duration=duration, date=date)

    for result_id in result:
        return result_id[0]


def create_relation_match_in_tournament(tx, match_id, tournament, year):
    tx.run("MATCH (m:Match), (t:Tournament) WHERE ID(m) = $match_id AND t.name = $tournament AND t.year = $year " +
           "CREATE (m)-[r:IN_TOURNAMENT]->(t)", match_id=match_id, tournament=tournament, year=year)


def create_relation_team_win_match(tx, match_id, team_name):
    tx.run("MATCH (m:Match), (t:Team) WHERE ID(m) = $match_id AND t.name = $team_name " +
           "CREATE (t)-[r:WIN]->(m)", match_id=match_id, team_name=team_name)


def create_relation_team_lose_match(tx, match_id, team_name):
    tx.run("MATCH (m:Match), (t:Team) WHERE ID(m) = $match_id AND t.name = $team_name " +
           "CREATE (t)-[r:LOSE]->(m)", match_id=match_id, team_name=team_name)


def load_circuits(session):
    session.execute_write(add_circuit, "AVP")
    session.execute_write(add_circuit, "FIVB")


def load_countries(session):
    file = open(set_file, encoding="utf8")
    csvreader = csv.reader(file)
    next(csvreader)

    countries = set()
    for x in csvreader:
        countries.add(x[2])
        countries.add(x[11])
        countries.add(x[16])

    file.close()

    for country in countries:
        session.execute_write(add_country, country)


def load_tournaments(session):
    file = open(set_file, encoding="utf8")
    csvreader = csv.reader(file)
    next(csvreader)

    tournaments = set()
    for x in csvreader:
        tournaments.add(json.dumps({"name": x[1], "year": x[3], "country": x[2], "circuit": x[0]}))

    file.close()

    for tournament_str in tournaments:
        tournament = json.loads(tournament_str)
        session.execute_write(add_tournament, tournament["name"], tournament["year"])
        session.execute_write(create_relation_tournament_country, tournament["name"], tournament["year"],
                              tournament["country"])
        session.execute_write(create_relation_tournament_circuit, tournament["name"], tournament["year"],
                              tournament["circuit"])


def load_players(session):
    file = open(set_file, encoding="utf8")
    csvreader = csv.reader(file)
    next(csvreader)

    players = set()
    for x in csvreader:
        players.add(json.dumps({"name": x[7], "gender": x[5], "birthdate": x[8], "height": x[10], "country": x[11]}))
        players.add(json.dumps({"name": x[12], "gender": x[5], "birthdate": x[13], "height": x[15], "country": x[16]}))
        players.add(json.dumps({"name": x[23], "gender": x[5], "birthdate": x[24], "height": x[26], "country": x[27]}))
        players.add(json.dumps({"name": x[18], "gender": x[5], "birthdate": x[19], "height": x[21], "country": x[22]}))

    file.close()

    for player_str in players:
        player = json.loads(player_str)
        session.execute_write(add_player, player["name"], player["gender"], player["birthdate"], player["height"])
        session.execute_write(create_relation_player_country, player["name"], player["gender"], player["birthdate"],
                              player["country"])


def load_teams(session):
    file = open(set_file, encoding="utf8")
    csvreader = csv.reader(file)
    next(csvreader)

    teams = set()
    for x in csvreader:
        teams.add(json.dumps({"name": f"{x[7]} + {x[12]}", "player1": x[7], "player1_birthdate": x[8], "player2": x[12],
                              "player2_birthdate": x[13]}))
        teams.add(
            json.dumps({"name": f"{x[18]} + {x[23]}", "player1": x[18], "player1_birthdate": x[19], "player2": x[23],
                        "player2_birthdate": x[24]}))

    file.close()

    for team_str in teams:
        team = json.loads(team_str)
        session.execute_write(add_team, team["name"])
        session.execute_write(create_relation_team_member, team["name"], team["player1"], team["player1_birthdate"],
                              team["player2"], team["player2_birthdate"])


def load_matches(session):
    file = open(set_file, encoding="utf8")
    csvreader = csv.reader(file)
    next(csvreader)

    matches = set()
    for x in csvreader:
        matches.add(json.dumps(
            {"win_team": f"{x[7]} + {x[12]}", "lose_team": f"{x[18]} + {x[23]}", "tournament": x[1], "year": x[3],
             "score": x[29], "duration": x[30], "date": x[4]}))

    file.close()

    for match_str in matches:
        match = json.loads(match_str)
        match_id = session.execute_write(add_match, match["score"], match["duration"], match["date"])
        session.execute_write(create_relation_match_in_tournament, match_id, match["tournament"], match["year"])
        session.execute_write(create_relation_team_win_match, match_id, match["win_team"])
        session.execute_write(create_relation_team_lose_match, match_id, match["lose_team"])


def print_n_biggest_tournaments(tx, n):
    query = "MATCH (m:Match)-[:IN_TOURNAMENT]->(t:Tournament) RETURN t.name, t.year, count(m) AS matches_count ORDER BY matches_count DESC LIMIT $n"
    for record in tx.run(query, n=n):
        print(f'{record[0]} - {record[1]} - {record[2]}')


def execute_print_n_biggest_tournaments(session, n):
    session.execute_read(print_n_biggest_tournaments, n=n)


def print_players_with_many_teams(tx):
    query = "MATCH (p:Player)-[:MEMBER_IN]->(t:Team) WITH p.name AS player_name, count(t) AS team_num WHERE team_num > 1 RETURN player_name, team_num ORDER BY team_num DESC"
    for record in tx.run(query):
        print(f'{record[0]} - {record[1]}')


def execute_print_players_with_many_teams(session):
    session.execute_read(print_players_with_many_teams)


def print_winners_in_age_range(tx, from_years, to_years):
    query = 'MATCH (p:Player)-[:MEMBER_IN]->(t:Team)-[:WIN]->(m:Match) WHERE datetime(m.date) - duration({years: $from_years}) >= datetime(p.birthdate) ' + \
            'AND datetime(m.date) - duration({years: $to_years}) < datetime(p.birthdate) RETURN DISTINCT p.name'
    for record in tx.run(query, from_years=from_years, to_years=to_years):
        print(f'{record[0]}')


def execute_print_winners_in_age_range(session, from_years, to_years):
    session.execute_read(print_winners_in_age_range, from_years, to_years)


def print_players_who_play_in_many_circuits(tx):
    query = 'MATCH (p:Player)-[:MEMBER_IN]->(t:Team)-[:WIN|LOSE]->(m:Match)-[:IN_TOURNAMENT]->(tour:Tournament)-[:IN_CIRCUIT]->(c:Circuit) ' + \
            'WITH p.name AS player, COUNT(DISTINCT c.name) AS circuits_num ' + \
            'WHERE circuits_num > 1 ' + \
            'RETURN DISTINCT player, circuits_num ORDER BY circuits_num DESC'
    for record in tx.run(query):
        print(f'{record[0]} - {record[1]}')


def execute_print_players_who_play_in_many_circuits(session):
    session.execute_read(print_players_who_play_in_many_circuits)


def print_players_with_name_contains(tx, name_part):
    query = 'MATCH (p:Player) WHERE p.name CONTAINS $name_part RETURN p'
    for record in tx.run(query, name_part=name_part):
        x = \
            str(record).split("properties=")[1][:-2].replace('"', '\'').replace(' ', '').split("\'name\':")[1].split(
                ',')[
                0][1:-1]
        print(f'{x}')


def execute_print_players_with_name_contains(session, name_part):
    session.execute_read(print_players_with_name_contains, name_part)


with driver.session(database="neo4j") as session:
    # !!! BEGIN SECTION WITH CREATING DATABASE !!!
    session.execute_write(delete_all)

    load_circuits(session)
    load_countries(session)
    load_tournaments(session)
    load_players(session)
    load_teams(session)
    load_matches(session)
    # !!! END SECTION WITH CREATING DATABASE !!!

    # !!! BEGIN TESTING SECTION !!!
    # execute_print_n_biggest_tournaments(session, 5)
    # execute_print_players_with_many_teams(session)
    # execute_print_winners_in_age_range(session, 20, 25)
    # execute_print_players_who_play_in_many_circuits(session)
    # execute_print_players_with_name_contains(session, "O\'")

driver.close()
