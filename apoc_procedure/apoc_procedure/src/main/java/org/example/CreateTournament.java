package org.example;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

public class CreateTournament {
    @Context
    public Transaction txn;

    @Context
    public Log log;

    @Procedure(value = "org.example.createTournament", mode = Mode.WRITE)
    @Description("dadada")
    public Stream<ProcResult> createTournament(@Name("name") String name,
                                               @Name("year") String year,
                                               @Name("country") String country,
                                               @Name("circuit") String circuit) {
        Optional<Node> circuitOptional = txn.findNodes(Label.label("Circuit"), new HashMap<String, Object>() {
            {
                put("name", circuit);
            }
        }).stream().findFirst();

        Node circuitN;
        if (!circuitOptional.isPresent()) {
            circuitN = txn.createNode(Label.label("Circuit"));
            circuitN.setProperty("name", circuit);
        } else {
            circuitN = circuitOptional.get();
        }

        Optional<Node> countryOptional = txn.findNodes(Label.label("Country"), new HashMap<String, Object>() {
            {
                put("name", country);
            }
        }).stream().findFirst();

        Node countryN;
        if (!countryOptional.isPresent()) {
            countryN = txn.createNode(Label.label("Country"));
            countryN.setProperty("name", country);
        } else {
            countryN = countryOptional.get();
        }

        Optional<Node> tournamentOptional = txn.findNodes(Label.label("Tournament"), new HashMap<String, Object>() {
            {
                put("name", country);
                put("year", year);
            }
        }).stream().findFirst();

        Node tournamentN;
        if (!tournamentOptional.isPresent()) {
            tournamentN = txn.createNode(Label.label("Tournament"));
            tournamentN.setProperty("name", name);
            tournamentN.setProperty("year", year);
        } else {
            tournamentN = tournamentOptional.get();
        }

        if (!tournamentOptional.isPresent())
        {
            tournamentN.createRelationshipTo(circuitN, RelationshipType.withName("IN_CIRCUIT"));
            tournamentN.createRelationshipTo(countryN, RelationshipType.withName("IN_COUNTRY"));
        }

        return Stream.of(new ProcResult(tournamentN.getElementId()));
    }

    public class ProcResult {
        public String id;

        public ProcResult(String id) {
            this.id = id;
        }
    }
}
