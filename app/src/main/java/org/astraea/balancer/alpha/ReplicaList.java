package org.astraea.balancer.alpha;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ReplicaList implements List<LogPlacement> {

  private final List<LogPlacement> logPlacements;

  public ReplicaList(List<LogPlacement> logPlacements) {
    this.logPlacements = logPlacements;
  }

  @Override
  public int size() {
    return logPlacements.size();
  }

  @Override
  public boolean isEmpty() {
    return logPlacements.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return logPlacements.contains(o);
  }

  @Override
  public Iterator<LogPlacement> iterator() {
    return logPlacements.iterator();
  }

  @Override
  public Object[] toArray() {
    return logPlacements.toArray();
  }

  @Override
  public <T> T[] toArray(T[] ts) {
    return logPlacements.toArray(ts);
  }

  @Override
  public boolean add(LogPlacement logPlacement) {
    return logPlacements.add(logPlacement);
  }

  @Override
  public boolean remove(Object o) {
    return logPlacements.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> collection) {
    //noinspection SlowListContainsAll
    return logPlacements.containsAll(collection);
  }

  @Override
  public boolean addAll(Collection<? extends LogPlacement> collection) {
    return logPlacements.addAll(collection);
  }

  @Override
  public boolean addAll(int i, Collection<? extends LogPlacement> collection) {
    return logPlacements.addAll(i, collection);
  }

  @Override
  public boolean removeAll(Collection<?> collection) {
    return logPlacements.removeAll(collection);
  }

  @Override
  public boolean retainAll(Collection<?> collection) {
    return logPlacements.retainAll(collection);
  }

  @Override
  public void clear() {
    logPlacements.clear();
  }

  @Override
  public LogPlacement get(int i) {
    return logPlacements.get(i);
  }

  @Override
  public LogPlacement set(int i, LogPlacement logPlacement) {
    return logPlacements.set(i, logPlacement);
  }

  @Override
  public void add(int i, LogPlacement logPlacement) {
    logPlacements.add(i, logPlacement);
  }

  @Override
  public LogPlacement remove(int i) {
    return logPlacements.remove(i);
  }

  @Override
  public int indexOf(Object o) {
    return logPlacements.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return logPlacements.lastIndexOf(o);
  }

  @Override
  public ListIterator<LogPlacement> listIterator() {
    return logPlacements.listIterator();
  }

  @Override
  public ListIterator<LogPlacement> listIterator(int i) {
    return logPlacements.listIterator(i);
  }

  @Override
  public List<LogPlacement> subList(int i, int i1) {
    return logPlacements.subList(i, i1);
  }

  public boolean containsReplicaAtBroker(int brokerId) {
    return logPlacements.stream().anyMatch(x -> x.broker() == brokerId);
  }
}
