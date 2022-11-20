package capstone;

import java.util.HashMap;

// HUGE assumption here: V=4
// future work will include working out the distance calculations for other values
// or deciding whether to overlay VPs on top of each other.
public enum Position {
    RIGHT(0),
    LEFT(1),
    TOP(2),
    BOTTOM(3);

    private static final HashMap<Integer, Position> BY_LABEL = new HashMap<>();
    
    static {
        for (Position p: values()) {
            BY_LABEL.put(p.order, p);
        }
    }

    public final int order;

    private Position(int order) {
        this.order = order;
    }

    public static Position valueOfLabel(Integer label) {
        return BY_LABEL.get(label);
    }
}
