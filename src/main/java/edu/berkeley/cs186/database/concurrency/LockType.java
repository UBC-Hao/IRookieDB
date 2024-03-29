package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held


    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // proj4_part1: implement
        if (a == NL || b == NL) return true;

        if (a == S){
            switch (b){
                    case S:
                        return true;
                    case IS:
                        return true;
                    default:
                        return false;
            }
        }

        if (a == IX){
            switch(b){
                case S:
                    return false;
                case X:
                    return false;
                case IS:
                    return true;
                case IX:
                    return true;
                case SIX:
                    return false;
            }
        }

        if (a == SIX){
            switch(b){
                case S:
                    return false;
                case X:
                    return false;
                case IS:
                    return true;
                case IX:
                    return false;
                case SIX:
                    return false;
            }
        }

        if (a==X){
            return false;
        }

        if (a==IS){
            switch (b){
                case S:
                    return true;
                case IS:
                    return true;
                case IX:
                    return true;
                case SIX:
                    return true;
                default:
                    return false;
            }
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // proj4_part1: implement
        if (parentLockType == IS){
            switch (childLockType){
                case S:
                    return true;
                case IS:
                    return true;
                case NL:
                    return true;
                default:
                    return false;
            }
        }

        if (parentLockType == S){
            switch (childLockType){
                case S:
                    return true;
                case NL:
                    return true;
                default:
                    return false;
            }
        }

        if (parentLockType == X){
            switch (childLockType){
                case X:
                    return true;
                case NL:
                    return true;
                default:
                    return false;
            }
        }

        if (parentLockType == IX){
            switch (childLockType){
                case S:
                    return true;
                case X:
                    return true;
                case NL:
                    return true;
                case IX:
                    return true;
                case SIX:
                    return true;
                case IS:
                    return true;
                default:
                    return false;
            }
        }

        if (parentLockType == SIX){
            switch (childLockType){

                case X:
                    return true;
                case NL:
                    return true;
                default:
                    return false;
            }
        }

        if (parentLockType == NL){
            switch (childLockType){
                case NL:
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // proj4_part1: implement
        if (required == NL) return true;
        if (required == S){
            switch (substitute){
                case IS:
                    return false;
                case NL:
                    return false;
                case IX:
                    return false;
                default:
                    return true;
            }
        }
        if (required == X){
            switch (substitute){
                case X:
                    return true;
                default:
                    return false;
            }
        }
        if (required == IS){
            switch (substitute){
                case S:
                case X:
                case IS:
                case IX:
                case SIX:
                    return true;
            }
            return false;
        }
        if (required == IX){
            switch (substitute){
                case X:
                case IX:
                case SIX:
                    return true;
            }
            return false;
        }
        if (required == SIX){
            switch (substitute){
                case X:
                case SIX:
                    return true;
            }
            return false;
        }
        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }


    public LockType toIntent(){
        if (this==LockType.X) return LockType.IX;
        else if (this==LockType.S) return LockType.IS;
        return LockType.NL;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

