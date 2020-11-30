package ch.unibas.dmi.dbis.fds.p2p.chord.impl;

import ch.unibas.dmi.dbis.fds.p2p.chord.api.*;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.ChordNetwork;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.data.Identifier;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.data.IdentifierCircle;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.data.IdentifierCircularInterval;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.math.HashFunction;
import jdk.jshell.spi.ExecutionControl;

import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static ch.unibas.dmi.dbis.fds.p2p.chord.api.data.IdentifierCircularInterval.createOpen;

/**
 * TODO: write JavaDoc
 *
 * @author loris.sauter
 */
public class ChordPeer extends AbstractChordPeer {
  /**
   *
   * @param identifier
   * @param network
   */
  protected ChordPeer(Identifier identifier, ChordNetwork network) {
    super(identifier, network);
  }

  /**
   * Asks this {@link ChordNode} to find {@code id}'s successor {@link ChordNode}.
   *
   * Defined in [1], Figure 4
   *
   * @param caller The calling {@link ChordNode}. Used for simulation - not part of the actual chord definition.
   * @param id The {@link Identifier} for which to lookup the successor. Does not need to be the ID of an actual {@link ChordNode}!
   * @return The successor of the node {@code id} from this {@link ChordNode}'s point of view
   */
  @Override
  public ChordNode findSuccessor(ChordNode caller, Identifier id) {
    /* TODO: DONE Implementation required. */
    ChordNode np = findPredecessor(caller,id);
    return np.successor();
  //  throw new RuntimeException("This method has not been implemented!");
  }

  /**
   * Asks this {@link ChordNode} to find {@code id}'s predecessor {@link ChordNode}
   *
   * Defined in [1], Figure 4
   *
   * @param caller The calling {@link ChordNode}. Used for simulation - not part of the actual chord definition.
   * @param id The {@link Identifier} for which to lookup the predecessor. Does not need to be the ID of an actual {@link ChordNode}!
   * @return The predecessor of or the node {@code of} from this {@link ChordNode}'s point of view
   */
  @Override
  public ChordNode findPredecessor(ChordNode caller, Identifier id) {
    /* TODO: DONE Implementation required. */
    ChordNode np = this;
    while(!IdentifierCircularInterval.createLeftOpen(np.id(),np.successor().id()).contains(id))
      np = closestPrecedingFinger(np, id);
    return np;
    //throw new RuntimeException("This method has not been implemented!");
  }
public  Identifier createIdentifier(int index)
{
  return new IdentifierCircle(getNetwork().getNbits()).getIdentifierAt(index);
}
  /**
   * Return the closest finger preceding the  {@code id}
   *
   * Defined in [1], Figure 4
   *
   * @param caller The calling {@link ChordNode}. Used for simulation - not part of the actual chord definition.
   * @param id The {@link Identifier} for which the closest preceding finger is looked up.
   * @return The closest preceding finger of the node {@code of} from this node's point of view
   */
  @Override
  public ChordNode closestPrecedingFinger(ChordNode caller, Identifier id) {
    /* TODO: Implementation required. */
    for(int i = this.getNetwork().getNbits(); i>0; i--)
    {
      ChordNode fingerinode = caller.finger().node(i).get();
      if(IdentifierCircularInterval.createOpen(caller.getIdentifier(),id).contains(fingerinode.getIdentifier()))
        return fingerinode;
    }
    return this;
    //throw new RuntimeException("This method has not been implemented!");
  }
  /**
   * Called on this {@link ChordNode} if it wishes to join the {@link ChordNetwork}. {@code nprime} references another {@link ChordNode}
   * that is already member of the {@link ChordNetwork}.
   *
   * Required for static {@link ChordNetwork} mode. Since no stabilization takes place in this mode, the joining node must make all
   * the necessary setup.
   *
   * Defined in [1], Figure 6
   *
   * @param nprime Arbitrary {@link ChordNode} that is part of the {@link ChordNetwork} this {@link ChordNode} wishes to join.
   */
  @Override
  public void joinAndUpdate(ChordNode nprime) {
    if (nprime != null) {
      initFingerTable(nprime);
      updateOthers();
      /* TODO: THIS IS WRONG I HAVE TO CHANGE!!!! Move keys. */
      Set<String> succkeys = successor().keys();
      HashFunction hashFunction = new HashFunction(getNetwork().getNbits());
      for (String k :
              succkeys) {
        if(IdentifierCircularInterval.createLeftOpen(successor().id(), id()).contains(createIdentifier(hashFunction.hash(k))))
          this.store(this, k, successor().forceDelete(findSuccessor(this,createIdentifier(0)), k).get());

      }
    } else {
      for (int i = 1; i <= getNetwork().getNbits(); i++) {
        this.fingerTable.setNode(i, this);
      }
      this.setPredecessor(this);
    }
  }

  /**
   * Called on this {@link ChordNode} if it wishes to join the {@link ChordNetwork}. {@code nprime} references
   * another {@link ChordNode} that is already member of the {@link ChordNetwork}.
   *
   * Required for dynamic {@link ChordNetwork} mode. Since in that mode {@link ChordNode}s stabilize the network
   * periodically, this method simply sets its successor and waits for stabilization to do the rest.
   *
   * Defined in [1], Figure 7
   *
   * @param nprime Arbitrary {@link ChordNode} that is part of the {@link ChordNetwork} this {@link ChordNode} wishes to join.
   */
  @Override
  public void joinOnly(ChordNode nprime) {
    setPredecessor(null);
    if (nprime == null) {
      this.fingerTable.setNode(1, this);
    } else {
      this.fingerTable.setNode(1, nprime.findSuccessor(this,this));
    }
    for(int i = 1; i < getNetwork().getNbits(); i++)
    {
      if(IdentifierCircularInterval.createRightOpen(id(), fingerTable.node(i).get().id()).contains(createIdentifier(fingerTable.start(i+1))))
        fingerTable.setNode(i+1, fingerTable.node(i).get());
      else
        fingerTable.setNode(i+1, successor().findSuccessor(this, createIdentifier(fingerTable.start(i+1))));
    }
  }

  /**
   * Initializes this {@link ChordNode}'s {@link FingerTable} based on information derived from {@code nprime}.
   *
   * Defined in [1], Figure 6
   *
   * @param nprime Arbitrary {@link ChordNode} that is part of the network.
   */
  private void initFingerTable(ChordNode nprime) {
    /* TODO: DONE Implementation required. */
    this.fingerTable.setNode(1, nprime.findSuccessor(this, createIdentifier(fingerTable.start(1))));
    this.setPredecessor(successor().predecessor());
    successor().setPredecessor(this);
    for(int i = 1; i < getNetwork().getNbits(); i++)
    {
      if(IdentifierCircularInterval.createRightOpen(id(), fingerTable.node(i).get().id()).contains(createIdentifier(fingerTable.start(i+1))))
        fingerTable.setNode(i+1, fingerTable.node(i).get());
      else
        fingerTable.setNode(i+1, nprime.findSuccessor(this, createIdentifier(fingerTable.start(i+1))));
    }
    //throw new RuntimeException("This method has not been implemented!");
  }

  /**
   * Updates all {@link ChordNode} whose {@link FingerTable} should refer to this {@link ChordNode}.
   *
   * Defined in [1], Figure 6
   */
  private void updateOthers() {
    /* TODO: DONE Implementation required. */
    //setPredecessor(findPredecessor(this,id()));

    for(int i=1;i<=getNetwork().getNbits(); i++)
    {
      ChordNode predecesor = findPredecessor(this, createIdentifier ((int)(id().getIndex()+1-java.lang.Math.pow(2, i-1 ))));// finger().node(i).get();
      predecesor.updateFingerTable(this,i);
    }
    //throw new RuntimeException("This method has not been implemented!");
  }
  /**
   * If node {@code s} is the i-th finger of this node, update this node's finger table with {@code s}
   *
   * Defined in [1], Figure 6
   *
   * @param s The should-be i-th finger of this node
   * @param i The index of {@code s} in this node's finger table
   */
  @Override
  public void updateFingerTable(ChordNode s, int i) {
    finger().node(i).ifPresent(node -> {
      /* TODO: DONE Implementation required. */
      if(IdentifierCircularInterval.createOpen(createIdentifier(id().getIndex()/*+(int)Math.pow(2,i-1)*/), finger().node(i).get().id()).contains(s.id()))//
      {
          fingerTable.setNode(i, s);
          ChordNode p=predecessor();
          p.updateFingerTable(s, i);
      }
      //throw new RuntimeException("This method has not been implemented!");
    });
  }

  /**
   * Called by {@code nprime} if it thinks it might be this {@link ChordNode}'s predecessor. Updates predecessor
   * pointers accordingly, if required.
   *
   * Defined in [1], Figure 7
   *
   * @param nprime The alleged predecessor of this {@link ChordNode}
   */
  @Override
  public void notify(ChordNode nprime) {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;

    /* TODO: DONE Implementation required. Hint: Null check on predecessor! */
    if(predecessor()==null || IdentifierCircularInterval.createOpen(predecessor().id(), id()).contains(nprime.id()))
    {
      setPredecessor(nprime);
    }
    //throw new RuntimeException("This method has not been implemented!");dwadwadwa

  }

  /**
   * Called periodically in order to refresh entries in this {@link ChordNode}'s {@link FingerTable}.
   *
   * Defined in [1], Figure 7
   */
  @Override
  public void fixFingers() {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;

    /* TODO: DONE Implementation required */
    //int i = new Random().nextInt()%finger().size()+1;
    int i = 1 + new Random().nextInt(finger().size());
    fingerTable.setNode(i, findSuccessor(this, createIdentifier(finger().start(i))));
    //throw new RuntimeException("This method has not been implemented!");
  }

  /**
   * Called periodically in order to verify this node's immediate successor and inform it about this
   * {@link ChordNode}'s presence,
   *
   * Defined in [1], Figure 7
   */
  @Override
  public void stabilize() {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;
    /* TODO: DONE Implementation required.*/
    /*if(successor() == null) {
      ChordNode newSuccessor = fingerTable.node(2).get();
      fingerTable.setNode(1, newSuccessor);
      notify(fingerTable.node(1).get());
    }*/
    ChordNode x=successor().predecessor();
    if(x != null && IdentifierCircularInterval.createOpen(id(), successor().id()).contains(x.id()))
    {
      fingerTable.setNode(1, x);
    }

    successor().notify(this);
    //throw new RuntimeException("This method has not been implemented!");
  }

  /**
   * Called periodically in order to check activity of this {@link ChordNode}'s predecessor.
   *
   * Not part of [1]. Required for dynamic network to handle node failure.
   */
  @Override
  public void checkPredecessor() {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;

    /* TODO: Implementation required. Hint: Null check on predecessor! */
    if(this.predecessor() != null && this.predecessor().status() == NodeStatus.OFFLINE)
      this.setPredecessor(null);
    //throw new RuntimeException("This method has not been implemented!");
  }

  /**
   * Called periodically in order to check activity of this {@link ChordNode}'s successor.
   *
   * Not part of [1]. Required for dynamic network to handle node failure.
   */
  @Override
  public void checkSuccessor() {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;
    /* TODO: Implementation required. Hint: Null check on predecessor! */

    ChordNode newSuccessor = fingerTable.node(2).get();
    if(this.successor() != null && this.successor().status() == NodeStatus.OFFLINE)
      //this.fingerTable.setNode(1, null);
      fingerTable.setNode(1, newSuccessor);
      newSuccessor.notify(this);
    //throw new RuntimeException("This method has not been implemented!");
  }

  /**
   * Performs a lookup for where the data with the provided key should be stored.
   *
   * @return Node in which to store the data with the provided key.
   */
  @Override
  protected ChordNode lookupNodeForItem(String key) {
    /* TODO: Implementation required. Hint: Null check on predecessor! */
    HashFunction hashFunction = new HashFunction(getNetwork().getNbits());
    return findSuccessor(this,createIdentifier(hashFunction.hash(key)));
    //throw new RuntimeException("This method has not been implemented!");
  }

  @Override
  public String toString() {
    return String.format("ChordPeer{id=%d}", this.id().getIndex());
  }
}
