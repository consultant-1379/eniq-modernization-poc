package com.ericsson.kyo.mdr;

import java.util.ArrayList;
import java.util.HashMap;

public class pe2re {
    private HashMap<String, ArrayList<String>> Map4RE;
    private HashMap<String, ArrayList<String>> Map4colPerVersion;

    public void REntity() {
        Map4RE = new HashMap<String, ArrayList<String>>();
        Map4colPerVersion = new HashMap<String, ArrayList<String>>();
    }

    public HashMap<String, ArrayList<String>> getMap4RE() {
        return Map4RE;
    }

    public void setMap4RE(HashMap<String, ArrayList<String>> map4RE) {
        Map4RE = map4RE;
    }

    public HashMap<String, ArrayList<String>> getMap4colPerVersion() {
        return Map4colPerVersion;
    }

    public void setMap4colPerVersion(
            HashMap<String, ArrayList<String>> map4colPerVersion) {
        Map4colPerVersion = map4colPerVersion;
    }
}
