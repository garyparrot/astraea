package org.astraea.balancer.log;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LogPlacementTest {

  @Test
  @DisplayName("Equals")
  void isMatch0() {
    final var placement0 =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var placement1 =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    Assertions.assertTrue(LogPlacement.isMatch(placement0, placement1));
  }

  @Test
  @DisplayName("Wrong order")
  void noMatch0() {
    final var placement0 =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var placement1 =
        List.of(LogPlacement.of(1, "/B"), LogPlacement.of(0, "/A"), LogPlacement.of(2, "/C"));
    Assertions.assertFalse(LogPlacement.isMatch(placement0, placement1));
  }

  @Test
  @DisplayName("No log dir name match")
  void noMatch1() {
    final var placement0 =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var placement1 =
        List.of(LogPlacement.of(0, "/Aaa"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    Assertions.assertFalse(LogPlacement.isMatch(placement0, placement1));
  }

  @Test
  @DisplayName("Optional log dir")
  void noMatch2() {
    final var sourcePlacement =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var targetPlacement =
        List.of(LogPlacement.of(0), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    // don't care which log dir placement[0] will eventually be.
    Assertions.assertTrue(LogPlacement.isMatch(sourcePlacement, targetPlacement));
  }

  @Test
  @DisplayName("Optional log dir")
  void noMatch3() {
    final var sourcePlacement =
        List.of(LogPlacement.of(0), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var targetPlacement =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    // do care which log dir placement[0] will eventually be.
    Assertions.assertFalse(LogPlacement.isMatch(sourcePlacement, targetPlacement));
  }
}
