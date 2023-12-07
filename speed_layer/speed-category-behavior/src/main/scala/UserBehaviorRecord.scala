import scala.reflect.runtime.universe._


case class UserBehaviorRecord(
                                        user_id: String,
                                        item_id: String,
                                        category_id: String,
                                        pv: Boolean,
                                        fav: Boolean,
                                        cart: Boolean,
                                        buy: Boolean)